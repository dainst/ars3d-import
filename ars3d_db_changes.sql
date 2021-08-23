
-- To undo: ALTER TABLE modell3d DROP COLUMN TechnischeHinweise;
ALTER TABLE modell3d ADD COLUMN TechnischeHinweise TEXT COLLATE utf8_unicode_ci AFTER Modellierer;

-- To undo: DELETE FROM URIQuelle WHERE Name LIKE 'Portal ARS3D';
INSERT INTO URIQuelle (Name, URLSchema) VALUES ('Portal ARS3D', 'http://143.93.113.149/_portal/object.htm?id=ars3do:');

-- To undo
--  UPDATE literatur SET ZenonID = NULL WHERE PS_LiteraturID = 17657;
--  DELETE FROM literatur WHERE ZenonID = '001053710';
UPDATE literatur SET ZenonID = '000009465' WHERE PS_LiteraturID = 17657;
INSERT INTO literatur SET
    ZenonID = '001053710',
    Abkuerzungen = 'Atlante 1981',
    DAIRichtlinien = 'Enciclopedia dell’Arte Antica classica e orientale, Atlante delle forme ceramiche I. Ceramica fine romana nel bacino mediterraneo (medio e tardo impero)',
    Jahr = '1981',
    Reihe = 'Enciclopedia dell’Arte Antica classica e orientale',
    Titel = 'Atlante delle forme ceramiche I. Ceramica fine romana nel bacino mediterraneo (medio e tardo impero)',
    StichwortSortierung = 'Atlante1981';
