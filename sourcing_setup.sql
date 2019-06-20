-- Creates tables 'events', 'users' and 'nodes'.
-- On insertion of an 'create', 'update' or 'delete'
-- row in the 'events' table. The corresponding
-- action is taken to update the targeted table.

DROP TABLE IF EXISTS "public"."events"; 
DROP SEQUENCE IF EXISTS "public"."events_id_seq";

CREATE SEQUENCE IF NOT EXISTS "public"."events_id_seq"
    INCREMENT BY 1
    MINVALUE 1 
    NO MAXVALUE
    START WITH 1;

CREATE TABLE "public"."events" ( 
	"id" BIGINT DEFAULT nextval('public.events_id_seq'::regclass) NOT NULL,
	"event" CHARACTER VARYING( 2044 ) NOT NULL,
	"type" CHARACTER VARYING( 2044 ),
	"data" jsonb NOT NULL DEFAULT '{}',
	"timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	"uuid" UUID,
	PRIMARY KEY ( "id" ) 
);

CREATE OR REPLACE FUNCTION "public"."on_event"(  ) RETURNS TRIGGER AS 
$function$
DECLARE 
    id BIGINT;
    data_type CHARACTER VARYING(2044);
    query_1 CHARACTER VARYING(2044) := '';
    query_2 CHARACTER VARYING(2044) := '';
    rec record;
    count INT := 1;
BEGIN
    -- NEW.id        - id of the event in the event table
    -- NEW.event     - action triggered by the event, can be 'create', 'update', 'delete' or 'rollback'
    -- NEW.type      - name of the table in which the operations will be triggered
    -- NEW.data      - data used to create/update the target row
    -- NEW.timestamp - timestamp of the event
    -- NEW.uuid      - uuid used to identify the object in the target table
    CASE NEW.event
    WHEN 'create' THEN
    	-- if no uuid is provided, a new one is generated
        CASE WHEN NEW.uuid IS NULL THEN
            NEW.uuid := uuid_generate_v4();
        ELSE
        END CASE;
        
	-- if no timestamp is provided, a new one is generated
        CASE WHEN NEW.timestamp IS NULL THEN
            NEW.timestamp := now();
        ELSE
        END CASE;
        
	-- if a password is provided, its hash is stored, while the password is removed
        CASE WHEN (NEW.data ? 'password') = TRUE THEN
            NEW.data := jsonb_set(NEW.data, '{hash}'::TEXT[], to_jsonb(crypt(NEW.data->>'password', gen_salt('bf'))));
            NEW.data := NEW.data - 'password';
        ELSE
        END CASE;
        
	-- An INSERT query is built from the columns of the target table. 
 	-- The fields in the NEW.data objects are cast to the types of the column.
	-- One limitation of the current implementation is that ENUM, ARRAY, and other CUSTOM TYPES are not supported.
        FOR rec IN (
            SELECT "columns"."column_name" AS "col_name", "columns"."data_type" AS "col_type"
                FROM "information_schema"."columns"
                WHERE "table_schema" = 'public' AND "table_name" = NEW.type
        )
        LOOP
            CASE WHEN (NEW.data ? rec.col_name) = TRUE THEN
                query_1 = query_1 || ', ' || quote_ident(rec.col_name);
                query_2 = query_2 || ', CAST(' || quote_literal((NEW.data->>rec.col_name)) || ' AS ' || rec.col_type || ')';
            ELSE
                CASE rec.col_name
                WHEN 'updated' THEN
                    query_1 = query_1 || ', ' || quote_ident(rec.col_name);
                    query_2 = query_2 || ', CAST(' || quote_literal(NEW.timestamp) || ' AS ' || rec.col_type || ')';
                ELSE
                END CASE;
            END CASE;
        END LOOP;
													     
	-- The INSERT is executed and the id of the newly created row is added to NEW.data
	-- Storing the new id is essential for the rollback feature when using foreign keys.
        EXECUTE 'INSERT INTO ' || quote_ident(NEW.type) 
            || '("uuid"' || query_1 || ')'
            || ' VALUES (' || quote_literal(NEW.uuid) || query_2 || ') RETURNING "id"'
        INTO id;
        NEW.data := jsonb_set(NEW.data, '{id}'::TEXT[], to_jsonb(id::INT));
    WHEN 'update' THEN
	-- The updates proceeds according to the same model as the creates, with the query being an UPDATE.
        CASE WHEN NEW.timestamp IS NULL THEN
            NEW.timestamp := now();
        ELSE
        END CASE;
        
        CASE WHEN (NEW.data ? 'password') = TRUE THEN
            NEW.data := jsonb_set(NEW.data, '{hash}'::TEXT[], to_jsonb(crypt(NEW.data->>'password', gen_salt('bf'))));
            NEW.data := NEW.data - 'password';
        ELSE
        END CASE;
        
        FOR rec IN (
            SELECT "columns"."column_name" AS "col_name", "columns"."data_type" AS "col_type"
                FROM "information_schema"."columns"
                WHERE "table_schema" = 'public' AND "table_name" = NEW.type
        )
        LOOP
            CASE WHEN (NEW.data ? rec.col_name) = TRUE THEN
                query_1 = query_1 || ', ' || quote_ident(rec.col_name) || ' = CAST(' || quote_literal((NEW.data->>rec.col_name)) || ' AS ' || rec.col_type || ')';
            ELSE
                CASE rec.col_name
                WHEN 'updated' THEN
                    query_1 = query_1 || ', updated = CAST(' || quote_literal(NEW.timestamp::TEXT) || ' AS ' || rec.col_type || ')';
                ELSE
                END CASE;
            END CASE;
        END LOOP;
        EXECUTE 'UPDATE ' || quote_ident(NEW.type)
            || ' SET ' || RIGHT(query_1, -2)
            || ' WHERE "uuid" = ' || quote_literal(NEW.uuid);
        GET DIAGNOSTICS count = ROW_COUNT;
            
    WHEN 'delete' THEN
	-- DELETEs simply delete a row from the target table
        EXECUTE 'DELETE FROM ' || quote_ident(NEW.type)
            || ' WHERE "uuid" = ' || quote_literal(NEW.uuid); 
        GET DIAGNOSTICS count = ROW_COUNT;
    WHEN 'rollback[end]' THEN
	-- rollback[end] is used as a marker to surround the event inserted by a rollback
    WHEN 'rollback' THEN
	-- rollbacks will erase the state from the database and replay the timeline until a criterion is matched
	-- the first step is to select all objects that haven't been deleted and to delete them.
        INSERT INTO "sourcing"."event" ("event", "type", "uuid", "timestamp") (
            SELECT 'delete' AS "event", "type" , "uuid", NEW.timestamp AS "timestamp"
                FROM "sourcing"."event" 
                GROUP BY "uuid", "type" 
                HAVING NOT (array_agg("event") @> '{delete}' OR "uuid" IS NULL)
                ORDER BY min("event"."id") ASC
        );
	-- two choices are given to identify at which event to stop: ID and TIMESTAMP
        CASE WHEN (NEW.data ? 'id') = TRUE THEN
	    -- ID inserts all events with an id smaller or equal to ID
            INSERT INTO "sourcing"."event" ("event", "type", "uuid", "data", "timestamp") (
                SELECT "event", "type", "uuid", "data", NEW.timestamp AS "timestamp"
                    FROM "sourcing"."event"
                    WHERE "event"."id" <= (NEW.data->>'id')::BIGINT
                    ORDER BY "event"."id" ASC
            );
        ELSE
	    -- TIMESTAMP inserts all events with a timestamp smaller or equal to TIMESTAMP
	    CASE WHEN (NEW.data ? 'timestamp') = TRUE THEN
	    	INSERT INTO "sourcing"."event" ("event", "type", "uuid", "data", "timestamp") (
		    SELECT "event", "type", "uuid", "data", NEW.timestamp AS "timestamp"
		    	FROM "sourcing"."event"
		    	WHERE "event"."timestamp" <= (NEW.data->>'timestamp')::TIMESTAMP WITHOUT TIME ZONE
		    	ORDER BY "event"."id" ASC
	    	);
	    ELSE
	        RAISE invalid_parameter_value USING MESSAGE = 'No criterion for the rollback was selected.';
	    END CASE; 
    	END CASE;       
        
	-- The rollback[end] is then finally inserted. NEW.event is also changed to rollback[begin]
     	INSERT INTO "sourcing"."event" ("event", "data", "timestamp") VALUES ('rollback[end]', NEW.data, NEW.timestamp);
        NEW.event := 'rollback[begin]';
    ELSE
        RAISE unique_violation USING MESSAGE = 'Invalid event type ' || quote_literal(NEW.event);
    END CASE;
    
    IF count = 0 THEN
        RAISE unique_violation USING MESSAGE = 'No item matches the provided uuid ' || quote_literal(NEW.uuid);
    END IF;
    
    RETURN NEW;
END;
$function$
LANGUAGE plpgsql;

CREATE TRIGGER on_event_insert
  BEFORE INSERT
  ON "public"."events"
  FOR EACH ROW
  EXECUTE PROCEDURE "public"."on_event"();
  
