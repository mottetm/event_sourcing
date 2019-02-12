INSERT INTO "sourcing"."event" ( 
    "uuid", "event", "type", "data"
) 
VALUES (
    '1e006893-bfe2-4f5c-9549-389d6ef75376', 'create', 'user', '{"username": "jsmith", "email": "john.smith@email.com", "password": "1234"}'
);

INSERT INTO "sourcing"."event" ( 
    "uuid", "event", "type", "data"
) 
VALUES (
    '1e006893-bfe2-4f5c-9549-389d6ef75376', 'update', 'user', '{"password": "12345678"}'
);

INSERT INTO "sourcing"."event" ( 
    "uuid", "event", "type", "data"
) 
VALUES (
    '1e006893-bfe2-4f5c-9549-389d6ef75376', 'update', 'user', '{"password": "abcd1234"}'
);

INSERT INTO "sourcing"."event" ( 
    "uuid", "event", "type"
) 
VALUES (
    '1e006893-bfe2-4f5c-9549-389d6ef75376', 'delete', 'user'
);

INSERT INTO "sourcing"."event" ( 
    "event", "type", "data", "timestamp"
) 
VALUES (
    'create', 'user', '{"username": "johnS", "email": "john.smith@email.com", "password": "a1b2c3d4"}', now() + INTERVAL '60 second'
);

--INSERT INTO "sourcing"."event" ( 
--    "event", "data", "timestamp"
--) 
--VALUES (
--    'rollback', '{"id": 3}', now() + INTerval '60 second'
--);

INSERT INTO "sourcing"."event" ( 
    "event", "data", "timestamp"
) 
VALUES (
    'rollback', jsonb_set('{}'::JSONB, '{timestamp}'::TEXT[], to_jsonb((now() + INTERVAL '30 second')::TIMESTAMP WITHOUT TIME ZONE::TEXT)), now() + INTERVAL '90 second'
);
