-- stepd — schema 007: `key_hash_current` becomes nullable.
--
-- The column was NOT NULL, so registration had to put *something* in it, and
-- what it put was a hash of the app id: a value shaped exactly like a key digest
-- that verified nothing. An operator reading the column would reasonably
-- conclude a signing key was configured when none was, which is the worst
-- possible answer to a security question — confidently wrong.
--
-- NULL now means what it says: this server holds no signing key for this app,
-- its attempts go out unsigned, and a conforming app will reject them.

ALTER TABLE app_bindings ALTER COLUMN key_hash_current DROP NOT NULL;

-- Clear the placeholders written by earlier versions. Leaving them would keep
-- the misleading answer in place for exactly the deployments that already have
-- the problem.
UPDATE app_bindings SET key_hash_current = NULL
 WHERE key_hash_current = sha256(convert_to(app_id, 'UTF8'));

COMMENT ON COLUMN app_bindings.key_hash_current IS
'SHA-256 of the signing key this server uses for the app, or NULL if it holds
none. Never the plaintext key: a database dump must not be a working credential.';
