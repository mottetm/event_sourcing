-- Creates tables 'events', 'users' and 'nodes'.
-- On insertion of an 'create', 'update' or 'delete'
-- row in the 'events' table. The corresponding
-- action is taken to update the targeted table.

DROP TABLE IF EXISTS "public"."events"; 
DROP SEQUENCE IF EXISTS "public"."events_id_seq";

CREATE SEQUENCE IF NOT EXISTS events_id_seq
    INCREMENT BY 1
    MINVALUE 1 
    NO MAXVALUE
    START WITH 1;

CREATE TABLE "public"."events" ( 
	"id" Bigint DEFAULT nextval('events_id_seq'::regclass) NOT NULL,
	"event" Character Varying( 2044 ) NOT NULL,
	"type" Character Varying( 2044 ) NOT NULL,
	"data" json NOT NULL DEFAULT '{}',
	"timestamp" Timestamp Without Time Zone NOT NULL DEFAULT now(),
	"uuid" UUID NOT NULL,
	PRIMARY KEY ( "id" ) 
);

DROP TABLE IF EXISTS "public"."users"; 
DROP SEQUENCE IF EXISTS "public"."users_id_seq";

CREATE SEQUENCE IF NOT EXISTS users_id_seq
    INCREMENT BY 1
    MINVALUE 1 
    NO MAXVALUE
    START WITH 1;
    
CREATE TABLE "public"."users" ( 
	"id" Integer DEFAULT nextval('users_id_seq'::regclass) NOT NULL,
	"uuid" UUid NOT NULL,
	"username" Character Varying( 2044 ) NOT NULL,
	"email" Character Varying( 2044 ) NOT NULL,
	"password" Character Varying( 2044 ) NOT NULL,
	PRIMARY KEY ( "id" ),
	CONSTRAINT "unique_user_username" UNIQUE( "username" ),
	CONSTRAINT "unique_user_email" UNIQUE( "email" ),
	CONSTRAINT "unique_user_password" UNIQUE( "password" ),
	CONSTRAINT "unique_user_uuid" UNIQUE( "uuid" ) 
);

DROP TABLE IF EXISTS "public"."nodes"; 
DROP SEQUENCE IF EXISTS "public"."nodes_id_seq";

CREATE SEQUENCE IF NOT EXISTS nodes_id_seq
    INCREMENT BY 1
    MINVALUE 1 
    NO MAXVALUE
    START WITH 1;
    
CREATE TABLE "public"."nodes" ( 
	"id" Integer DEFAULT nextval('nodes_id_seq'::regclass) NOT NULL,
	"uuid" UUID,
	"value" jsonb NOT NULL DEFAULT '{}',
	"time" Timestamp Without Time Zone,
	PRIMARY KEY ( "id" ),
	CONSTRAINT "unique_node_uuid" UNIQUE( "uuid" ) 
);

CREATE OR REPLACE FUNCTION "public"."on_event"(  ) RETURNS Trigger AS 
$function$
DECLARE 
    data_type Character Varying(2044);
    query CHARACTER VARYING(2044) := '';
    query_1 CHARACTER VARYING(2044) := '';
    query_2 CHARACTER VARYING(2044) := '';
    rec record;
    count int := 1;
BEGIN
    CASE NEW.event
    WHEN 'create' THEN
        FOR rec in (
            SELECT field as cname, col.data_type as dtype
                FROM json_object_keys(NEW.data) as field
                LEFT JOIN information_schema.columns as col
                    ON field = col.column_name
                WHERE table_schema = 'public' AND table_name = NEW.type || 's'
        )
        LOOP
            query_1 = query_1 || ', ' || quote_ident(rec.cname);
            query_2 = query_2 || ', CAST(' || quote_literal((NEW.data->>rec.cname)) || ' AS ' || rec.dtype || ')';
        END LOOP;
        EXECUTE 'INSERT INTO ' || quote_ident(NEW.type || 's') 
            || '("uuid"' || query_1 || ')'
            || ' VALUES (' || quote_literal(NEW.uuid) || query_2 || ')';
    WHEN 'update' THEN 
        FOR rec in (
            SELECT field as cname, col.data_type as dtype
                FROM json_object_keys(NEW.data) as field
                LEFT JOIN information_schema.columns as col
                    ON field = col.column_name
                WHERE table_schema = 'public' AND table_name = NEW.type || 's'
        )
        LOOP
            query_1 = query_1 || ', ' || quote_ident(rec.cname) || ' ='
                || ' CAST(' || quote_literal((NEW.data->>rec.cname)) || ' AS ' || rec.dtype || ')';
        END LOOP;
        EXECUTE 'UPDATE ' || quote_ident(NEW.type || 's')
            || ' SET ' || right(query_1, -2)
            || ' WHERE "uuid" = ' || quote_literal(NEW.uuid);
        GET DIAGNOSTICS count = ROW_COUNT;
            
    WHEN 'delete' THEN
        EXECUTE 'DELETE FROM ' || quote_ident(NEW.type || 's')
            || ' WHERE "uuid" = ' || quote_literal(NEW.uuid); 
        GET DIAGNOSTICS count = ROW_COUNT;
    ELSE
        RAISE unique_violation USING MESSAGE = 'Invalid event type ' || quote_literal(NEW.event);
    END CASE;
    
    if count = 0 THEN
        RAISE unique_violation USING MESSAGE = 'No item matches the provided uuid ' || quote_literal(NEW.uuid);
    END IF;
    
    RETURN NEW;
END;
$function$
LANGUAGE plpgsql;

CREATE TRIGGER on_event_insert
  AFTER INSERT
  ON "public"."events"
  FOR EACH ROW
  EXECUTE PROCEDURE "public"."on_event"();
  
INSERT INTO "public"."events" (
    "event",
    "type",
    "data",
    "uuid"
) VALUES (
    'create',
    'user',
    '{"username": "mottetm", "email": "matthieu.mottet@outlook.com", "password": "my_hashed_password"}',
    '37afb78b-6470-4531-b265-1685b1c2f093'
);
 
INSERT INTO "public"."events" (
    "event",
    "type",
    "data",
    "uuid"
) VALUES (
    'create',
    'user',
    '{"username": "ttt", "email": "ttt@zurich.ibm.com", "password": "ttt_s_hashed_password"}',
    '8bfbf5c0-75bd-4ffd-95e1-69fa72421d8b'
);
 
INSERT INTO "public"."events" (
    "event",
    "type",
    "data",
    "uuid"
) VALUES (
    'create',
    'node',
    '{"value": {"user": {"username": "ttt", "email": "ttt@zurich.ibm.com", "password": "ttt_s_hashed_password"}, "hello": "World!"}, "info": "client#1234"}',
    '0f6622b8-ace0-4335-a956-4a67d42b2442'
);

INSERT INTO "public"."events" (
    "event",
    "type",
    "data",
    "uuid"
) VALUES (
    'update',
    'user',
    '{"username": "mmottet", "email": "m.mottet@outlook.com"}',
    '37afb78b-6470-4531-b265-1685b1c2f093'
);
 
INSERT INTO "public"."events" (
    "event",
    "type",
    "uuid"
) VALUES (
    'delete',
    'user',
    '8bfbf5c0-75bd-4ffd-95e1-69fa72421d8b'
);

SELECT * FROM "users";
