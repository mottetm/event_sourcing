DROP TABLE IF EXISTS "public"."todos"; 
DROP SEQUENCE IF EXISTS "public"."todos_id_seq";

DROP TABLE IF EXISTS "public"."users"; 
DROP SEQUENCE IF EXISTS "public"."users_id_seq";

CREATE SEQUENCE IF NOT EXISTS users_id_seq
    INCREMENT BY 1
    MINVALUE 1 
    NO MAXVALUE
    START WITH 1;
    
CREATE TABLE "public"."users" ( 
	"id" INTEGER DEFAULT nextval('users_id_seq'::regclass) NOT NULL,
	"uuid" UUid NOT NULL,
	"username" CHARACTER VARYING( 64 ) NOT NULL,
	"email" CHARACTER VARYING( 128 ) NOT NULL,
	"password" CHARACTER VARYING( 128 ) NOT NULL,
	PRIMARY KEY ( "id" ),
	CONSTRAINT "unique_user_username" UNIQUE( "username" ),
	CONSTRAINT "unique_user_email" UNIQUE( "email" ),
	CONSTRAINT "unique_user_password" UNIQUE( "password" ),
	CONSTRAINT "unique_user_uuid" UNIQUE( "uuid" ) 
);

CREATE SEQUENCE IF NOT EXISTS todos_id_seq
    INCREMENT BY 1
    MINVALUE 1 
    NO MAXVALUE
    START WITH 1;
    
CREATE TABLE "public"."todos" ( 
	"id" Bigserial NOT NULL,
	"uuid" UUid NOT NULL,
	"title" CHARACTER VARYING( 128 ) NOT NULL,
	"created" TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL,
	"updated" TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL,
	"status" BOOLEAN DEFAULT FALSE NOT NULL,
	"description" TEXT NOT NULL,
	"user_id" BIGINT NOT NULL,
	PRIMARY KEY ( "id" ),
	CONSTRAINT "unique_todos_uuid" UNIQUE( "uuid" ),
	CONSTRAINT "unique_todos_title" UNIQUE( "title", "user_id" ),
	CONSTRAINT "link_user_todos" 
        FOREIGN KEY ( "user_id" ) REFERENCES "public"."users" ( "id" ) MATCH FULL
        ON DELETE CASCADE ON UPDATE CASCADE
 );