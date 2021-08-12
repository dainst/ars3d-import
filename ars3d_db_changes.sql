
-- To undo: ALTER TABLE modell3d DROP COLUMN TechnischeHinweise;
ALTER TABLE modell3d ADD COLUMN TechnischeHinweise TEXT COLLATE utf8_unicode_ci AFTER Modellierer;

-- To undo: DELETE FROM URIQuelle WHERE Name LIKE 'Portal ARS3D';
INSERT INTO URIQuelle (Name, URLSchema) VALUES ('Portal ARS3D', 'http://143.93.113.149/_portal/object.htm?id=ars3do:');
