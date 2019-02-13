-- Creates tables 'events', 'users' and 'nodes'.
-- On insertion of an 'create', 'update' or 'delete'
-- row in the 'events' table. The corresponding
-- action is taken to update the targeted table.

DROP SCHEMA IF EXISTS "sourcing" CASCADE;
CREATE SCHEMA "sourcing";

DROP TABLE IF EXISTS "sourcing"."event"; 
DROP SEQUENCE IF EXISTS "sourcing"."event_id_seq";

CREATE SEQUENCE IF NOT EXISTS "sourcing"."event_id_seq"
    INCREMENT BY 1
    MINVALUE 1 
    NO MAXVALUE
    START WITH 1;

CREATE TABLE "sourcing"."event" ( 
	"id" BIGINT DEFAULT nextval('sourcing.event_id_seq'::regclass) NOT NULL,
	"event" CHARACTER VARYING( 2044 ) NOT NULL,
	"type" CHARACTER VARYING( 2044 ),
	"data" jsonb NOT NULL DEFAULT '{}',
	"timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	"uuid" UUID,
	PRIMARY KEY ( "id" ) 
);

CREATE OR REPLACE FUNCTION "sourcing"."on_event"(  ) RETURNS TRIGGER AS 
$function$
DECLARE 
    id BIGINT;
    data_type CHARACTER VARYING(2044);
    QUERY CHARACTER VARYING(2044) := '';
    query_1 CHARACTER VARYING(2044) := '';
    query_2 CHARACTER VARYING(2044) := '';
    rec record;
    count INT := 1;
BEGIN
    CASE NEW.event
    WHEN 'create' THEN
    
        CASE WHEN NEW.uuid IS NULL THEN
            NEW.uuid := uuid_generate_v4();
        ELSE
        END CASE;
        
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
                query_1 = query_1 || ', ' || quote_ident(rec.col_name);
                query_2 = query_2 || ', CAST(' || quote_literal((NEW.data->>rec.col_name)) || ' AS ' || rec.col_type || ')';
            ELSE
                CASE rec.col_name
                WHEN 'updated' THEN
                    CASE WHEN NEW.timestamp IS NULL THEN
                        NEW.timestamp := now();
                    ELSE
                    END CASE;
                    query_1 = query_1 || ', ' || quote_ident(rec.col_name);
                    query_2 = query_2 || ', CAST(' || quote_literal(NEW.timestamp) || ' AS ' || rec.col_type || ')';
                ELSE
                END CASE;
            END CASE;
        END LOOP;
        EXECUTE 'INSERT INTO ' || quote_ident(NEW.type) 
            || '("uuid"' || query_1 || ')'
            || ' VALUES (' || quote_literal(NEW.uuid) || query_2 || ') RETURNING "id"'
        INTO id;
        NEW.data := jsonb_set(NEW.data, '{id}'::TEXT[], to_jsonb(id::INT));
    WHEN 'update' THEN 
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
        EXECUTE 'DELETE FROM ' || quote_ident(NEW.type)
            || ' WHERE "uuid" = ' || quote_literal(NEW.uuid); 
        GET DIAGNOSTICS count = ROW_COUNT;
    WHEN 'rollback[end]' THEN
    WHEN 'rollback' THEN
        INSERT INTO "sourcing"."event" ("event", "type", "uuid", "timestamp") (
            SELECT 'delete' AS "event", "type" , "uuid", NEW.timestamp AS "timestamp"
                FROM "sourcing"."event" 
                GROUP BY "uuid", "type" 
                HAVING NOT (array_agg("event") @> '{delete}' OR "uuid" IS NULL)
                ORDER BY min("event"."id") ASC
        );
        CASE WHEN (NEW.data ? 'id') = TRUE THEN
            INSERT INTO "sourcing"."event" ("event", "type", "uuid", "data", "timestamp") (
                SELECT "event", "type", "uuid", "data", NEW.timestamp AS "timestamp"
                    FROM "sourcing"."event"
                    WHERE "event"."id" <= (NEW.data->>'id')::BIGINT
                    ORDER BY "event"."id" ASC
            );
        ELSE
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
  ON "sourcing"."event"
  FOR EACH ROW
  EXECUTE PROCEDURE "sourcing"."on_event"();
  
