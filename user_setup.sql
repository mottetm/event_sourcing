DROP TABLE IF EXISTS "public"."user"; 
DROP SEQUENCE IF EXISTS "public"."user_id_seq";

CREATE SEQUENCE IF NOT EXISTS user_id_seq
    INCREMENT BY 1
    MINVALUE 1 
    NO MAXVALUE
    START WITH 1;
    
CREATE TABLE "public"."user" ( 
	"id" INTEGER DEFAULT nextval('public.user_id_seq'::regclass) NOT NULL,
	"uuid" UUid NOT NULL,
	"updated" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	"username" CHARACTER VARYING( 64 ) NOT NULL,
	"email" CHARACTER VARYING( 128 ) NOT NULL,
	"hash" CHARACTER VARYING( 128 ) NOT NULL,
	PRIMARY KEY ( "id" ),
	CONSTRAINT "unique_user_username" UNIQUE( "username" ),
	CONSTRAINT "unique_user_email" UNIQUE( "email" ),
	CONSTRAINT "unique_user_uuid" UNIQUE( "uuid" ) 
);
