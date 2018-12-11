-- Creates tables 'events', 'users' and 'nodes'.
-- On insertion of an 'create', 'update' or 'delete'
-- row in the 'events' table. The corresponding
-- action is taken to update the targeted table.

DROP EXTENSION IF EXISTS "pgcrypto";
CREATE EXTENSION "pgcrypto";

DROP EXTENSION IF EXISTS "uuid-ossp";
CREATE EXTENSION "uuid-ossp";

DROP SCHEMA IF EXISTS "sourcing" CASCADE;
CREATE SCHEMA "sourcing";

DROP TABLE IF EXISTS "sourcing"."events"; 
DROP SEQUENCE IF EXISTS "sourcing"."events_id_seq";

CREATE SEQUENCE IF NOT EXISTS events_id_seq
    INCREMENT BY 1
    MINVALUE 1 
    NO MAXVALUE
    START WITH 1;

CREATE TABLE "sourcing"."events" ( 
	"id" BIGINT DEFAULT nextval('events_id_seq'::regclass) NOT NULL,
	"event" CHARACTER VARYING( 2044 ) NOT NULL,
	"type" CHARACTER VARYING( 2044 ) NOT NULL,
	"data" json NOT NULL DEFAULT '{}',
	"timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
	"uuid" UUID NOT NULL,
	PRIMARY KEY ( "id" ) 
);

CREATE OR REPLACE FUNCTION "sourcing"."on_event"(  ) RETURNS TRIGGER AS 
$function$
DECLARE 
    data_type CHARACTER VARYING(2044);
    query CHARACTER VARYING(2044) := '';
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
        FOR rec IN (
            SELECT field AS cname, col.data_type AS dtype
                FROM json_object_keys(NEW.data) AS field
                LEFT JOIN information_schema.columns AS col
                    ON field = col.column_name
                WHERE table_schema = 'public' AND table_name = NEW.type || 's'
        )
        
        LOOP
            query_1 = query_1 || ', ' || quote_ident(rec.cname);
            CASE rec.cname
            WHEN 'password' THEN
                NEW.data := jsonb_set(to_jsonb(NEW.data), '{password}'::TEXT[], to_jsonb(crypt(NEW.data->>rec.cname, gen_salt('bf'))));
            ELSE
            END CASE;
            query_2 = query_2 || ', CAST(' || quote_literal((NEW.data->>rec.cname)) || ' AS ' || rec.dtype || ')';
        END LOOP;
        EXECUTE 'INSERT INTO ' || quote_ident(NEW.type || 's') 
            || '("uuid"' || query_1 || ')'
            || ' VALUES (' || quote_literal(NEW.uuid) || query_2 || ')';
    WHEN 'update' THEN 
        CASE WHEN (to_jsonb(NEW.data) ? 'updated') = FALSE THEN
            NEW.data := jsonb_set(to_jsonb(NEW.data), '{updated}'::TEXT[], to_jsonb(NEW.timestamp));
        ELSE
        END CASE;
        FOR rec IN (
            SELECT field AS cname, col.data_type AS dtype
                FROM json_object_keys(NEW.data) AS field
                LEFT JOIN information_schema.columns AS col
                    ON field = col.column_name
                WHERE table_schema = 'public' AND table_name = NEW.type || 's'
        )
        LOOP
            CASE rec.cname
            WHEN 'password' THEN
                NEW.data := jsonb_set(to_jsonb(NEW.data), '{password}'::TEXT[], to_jsonb(crypt(NEW.data->>rec.cname, gen_salt('bf'))));
            ELSE
            END CASE;
            query_1 = query_1 || ', ' || quote_ident(rec.cname) || ' ='
                || ' CAST(' || quote_literal((NEW.data->>rec.cname)) || ' AS ' || rec.dtype || ')';
        END LOOP;
        EXECUTE 'UPDATE ' || quote_ident(NEW.type || 's')
            || ' SET ' || RIGHT(query_1, -2)
            || ' WHERE "uuid" = ' || quote_literal(NEW.uuid);
        GET DIAGNOSTICS count = ROW_COUNT;
            
    WHEN 'delete' THEN
        EXECUTE 'DELETE FROM ' || quote_ident(NEW.type || 's')
            || ' WHERE "uuid" = ' || quote_literal(NEW.uuid); 
        GET DIAGNOSTICS count = ROW_COUNT;
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
  ON "sourcing"."events"
  FOR EACH ROW
  EXECUTE PROCEDURE "sourcing"."on_event"();
  