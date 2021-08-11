
ALTER TABLE modell3d ADD COLUMN TechnischeHinweise TEXT COLLATE utf8_unicode_ci AFTER Modellierer;

-- To undo: ALTER TABLE modell3d DROP COLUMN TechnischeHinweise;
