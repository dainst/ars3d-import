#!/usr/bin/env python3

# Import data from the ARS3D project into arachne (from csv dump and possibly additional API calls)
# These imports are reversible with:
#   DELETE FROM objekt WHERE Arbeitsnotiz LIKE 'ARS3D-Import'; DELETE FROM modell3d WHERE Pfad LIKE '/ars3d-test%';

import argparse
import csv
import glob
import json
import os
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Optional, Sequence, Tuple

import requests
from mysql.connector import connect, MySQLConnection

DEFAULT_DB_CONF = os.path.join(os.path.dirname(__file__), '..', 'Config', 'db.my.cnf')

IMPORT_MARKER = 'ARS3D-Import'
CREATOR_NOTE = 'i3Mainz'
COPYRIGHT = 'Copyright i3mainz; Alle Rechte vorbehalten'
FOLDER_REMOTE = '/ars3d-test'

PORTAL_URI_TEMPLATE = 'http://143.93.113.149/mntModels/rgzm/ars3do/%s/%s.json'
PORTAL_LINK_TO_TEMPLATE = 'http://143.93.113.149/_portal/object.htm?id=ars3do:%s'
PORTAL_LINK_URI_QUELLE_ID = 45

IGNORE_FLAG = 'IGNORE'
REPLACE_FLAG = 'REPLACE'

MAPPING_OBJEKT = {
    'inventoryNumber': [('Katalognummer', REPLACE_FLAG)],
    'materialLabel': {
        'clay': [('Material', 'Ton')],
        'plaster': [('Material', 'Gips')],
    },
    'conditionLabel': {
        'complete': [('Erhaltung', 'vollständig')],
        'fragmented': [('Erhaltung', 'fragmentiert')],
        'reconstructed': [('Erhaltung', 'fragmentiert'), ('Bearbeitungen', 'ergänzt')],
    },
    'shapeLabel': {
        'bowl': [('GattungAllgemein', 'Kleinfund;Keramik;Gefäß')],
        'rectangular dish': [('GattungAllgemein', 'Kleinfund;Keramik;Gefäß')],
        'mould': [('GattungAllgemein', 'Kleinfund;Keramik')],
        'lamp': [('GattungAllgemein', 'Kleinfund;Keramik;Gerät')],
        'dish': [('GattungAllgemein', 'Kleinfund;Keramik;Gefäß')],
        'plate': [('GattungAllgemein', 'Kleinfund;Keramik;Gefäß')],
        'stamp': [('GattungAllgemein', 'Kleinfund;Keramik;Gerät')],
        'jug': [('GattungAllgemein', 'Kleinfund;Keramik;Gefäß')],
        "unclassified": [IGNORE_FLAG]
    },
    'manufacturingtypeLabel': {
        'potter wheel': [('Technik', 'Keramiktechnik, scheibengedreht')],
        'mould made': [('Technik', 'Keramiktechnik, Matritze')]
    }
}

MAPPING_OBJEKTKERAMIK = {
    'shapeLabel': {
        'bowl': [('GefaessformenKeramik', 'Schale')],
        'rectangular dish': [('GefaessformenKeramik', 'Teller')],
        'mould': [IGNORE_FLAG],
        'lamp': [IGNORE_FLAG],
        'dish': [('GefaessformenKeramik', 'Teller')],
        'plate': [('GefaessformenKeramik', 'Teller')],
        'stamp': [IGNORE_FLAG],
        'jug': [('GefaessformenKeramik', 'Kanne;Krug')],
        "unclassified": [IGNORE_FLAG]
    },
}

# Enumerate instead of parsing since cases are limited
MAPPING_DATES = {
    'dateLabel': {
        '310/320-430/450 AD': [('AnfDatZeitraum', '310/320'), ('AnfDatvn', 'n. Chr.'),
                               ('EndDatZeitraum', '430/450'), ('EndDatvn', 'n. Chr.')],
        '350/355 - 450/475 AD': [('AnfDatZeitraum', '350/355'), ('AnfDatvn', 'n. Chr.'),
                                 ('EndDatZeitraum', '450/475'), ('EndDatvn', 'n. Chr.')],
        '330-370 to early 5. century AD': [('AnfDatZeitraum', '330-370'), ('AnfDatvn', 'n. Chr.'),
                                           ('EndDatZeitraum', 'Anfang/frühes'),
                                           ('EndDatJh', '5'), ('EndDatvn', 'n. Chr.')],
        '280/300 to late 4. century AD': [('AnfDatZeitraum', '280/300'), ('AnfDatvn', 'n. Chr.'),
                                          ('EndDatZeitraum', 'Ende/spätes'),
                                          ('EndDatJh', '4'), ('EndDatvn', 'n. Chr.')],
        '260-320 or mid-late 4. century AD': [('AnfDatZeitraum', '260-320'), ('AnfDatvn', 'n. Chr.'),
                                              ('EndDatZeitraum', 'Ende/spätes'),
                                              ('EndDatJh', '4'), ('EndDatvn', 'n. Chr.')],
        'late 4. or early 5. century AD': [('AnfDatZeitraum', 'Ende/spätes'),
                                           ('AnfDatJh', '4'), ('AnfDatvn', 'n. Chr.'),
                                           ('EndDatZeitraum', 'Anfang/frühes'),
                                           ('EndDatJh', '5'), ('EndDatvn', 'n. Chr.')],
        'mid-late 4. century AD': [('EndDatZeitraum', 'Mitte/Ende'), ('EndDatJh', '4'), ('EndDatvn', 'n. Chr.')],
        'mid-late 5. century AD': [('EndDatZeitraum', 'Mitte/Ende'), ('EndDatJh', '5'), ('EndDatvn', 'n. Chr.')],
        'early 5. century AD': [('EndDatZeitraum', 'Anfang/frühes'), ('EndDatJh', '5'), ('EndDatvn', 'n. Chr.')],
        'early 6. century AD': [('EndDatZeitraum', 'Anfang/frühes'), ('EndDatJh', '6'), ('EndDatvn', 'n. Chr.')],
        '4. century AD': [('EndDatJh', '4'), ('EndDatvn', 'n. Chr.')],
        '5.century AD': [('EndDatJh', '4'), ('EndDatvn', 'n. Chr.')],
        '4.-5. century AD': [('AnfDatJh', '5'), ('AnfDatvn', 'n. Chr.'), ('EndDatJh', '5'), ('EndDatvn', 'n. Chr.')],
        '430-500 AD': [('AnfPraezise', '430'), ('AnfDatvn', 'n. Chr.'),
                       ('EndPraezise', '500'), ('EndDatvn', 'n. Chr.')],
        '420-450 AD': [('AnfPraezise', '420'), ('AnfDatvn', 'n. Chr.'),
                       ('EndPraezise', '450'), ('EndDatvn', 'n. Chr.')],
        '350-430 AD': [('AnfPraezise', '350'), ('AnfDatvn', 'n. Chr.'),
                       ('EndPraezise', '430'), ('EndDatvn', 'n. Chr.')],
        '290/300-375': [('AnfDatZeitraum', '290/300'), ('AnfDatvn', 'n. Chr.'),
                        ('EndPraezise', '430'), ('EndDatvn', 'n. Chr.')],
    }
}


@dataclass(frozen=True)
class ArachnePlaceReference:
    object_id: int
    place_id: int
    type_reference: str


class ConnectionWithDryRun:

    def __init__(self, connection: MySQLConnection, is_dry_run: bool):
        self.connection = connection
        self.dry_run = is_dry_run

    def insert(self, sql, params) -> int:
        sql_out = sql.replace('%s', '"%s"')
        print(sql_out % tuple(params))
        if self.dry_run:
            result = -1
        else:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                result = cursor.lastrowid
                self.connection.commit()
        if result is None:
            raise Exception('Unable to execute sql:', sql, params)
        return result

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()


def mapping_apply(mapping: dict, in_key: str, in_value: str) -> Sequence[Tuple[str, str]]:
    """
    Map one input key-value pair to zero or more output key-value pairs.
    """
    if not in_value.strip():
        return []
    mapped = mapping.get(in_key, [IGNORE_FLAG])
    if isinstance(mapped, dict):
        try:
            mapped = mapped[in_value.strip()]
        except KeyError:
            raise KeyError(f"Missing a mapping for '{in_key}.{in_value}'.")

    mapped = [t for t in mapped if t != IGNORE_FLAG]
    mapped = [(k, in_value) if v == REPLACE_FLAG else (k, v) for k, v in mapped]
    return mapped


def mapping_apply_all(mapping: dict, inputs: dict) -> Sequence[Tuple[str, str]]:
    mapped = [mapping_apply(mapping, k, v) for k, v in inputs.items()]
    return [t for m in mapped for t in m]


def arache_object_fields(row: dict) -> Sequence[Tuple[str, str]]:
    fields = mapping_apply_all(MAPPING_OBJEKT, row)
    title = row.get('objectLabel').capitalize()
    return [
        ('KurzbeschreibungObjekt', title),
        *fields,
        ('Arbeitsnotiz', IMPORT_MARKER),
        ('BearbeiterObjekt', CREATOR_NOTE)
    ]


def arachne_place_ref_fields(row: dict, object_id) -> Sequence[Tuple[str, str]]:
    if row.get('residenceLabel') != 'Römisch-Germanisches Zentralmuseum':
        raise Exception('Unexpected place of residence: ' + row.get('residenceLabel', ''))
    return [('FS_OrtID', 1591),
            ('FS_ObjektID', object_id),
            ('ArtOrtsangabe', 'Aufbewahrungsort'),
            ('Ursprungsinformationen', IMPORT_MARKER)]


def arachne_datierung_fields(row: dict, object_id) -> Sequence[Tuple[str, str]]:
    fields = mapping_apply_all(MAPPING_DATES, row)
    return [('FS_ObjektID', object_id), *fields, ('Ursprungsinformationen', IMPORT_MARKER)] if fields else []


def arachne_datierung_period_fields(row: dict, object_id) -> Sequence[Tuple[str, str]]:
    period_url = row.get('periodChronontology', '')
    period_label = row.get('periodLabel', '')
    if period_url and period_label:
        period_id = period_url.split('/')[-1]
        return [('FS_ObjektID', object_id),
                ('AnfEpoche', period_label),
                ('AnfEpocheChronId', period_id),
                ('Ursprungsinformationen', IMPORT_MARKER)]


def arachne_uri_fields(row: dict, object_id) -> Sequence[Tuple[str, str]]:
    uri = PORTAL_LINK_TO_TEMPLATE % row.get('object')
    return [
        ('FS_ObjektID', object_id),
        ('URI', uri),
        ('FS_URIQuelleID', PORTAL_LINK_URI_QUELLE_ID),
        ('Beziehung', 'sameAs'),
    ]


def arachne_objektkeramik_fields(row: dict, object_id) -> Sequence[Tuple[str, str]]:
    fields = mapping_apply_all(MAPPING_OBJEKTKERAMIK, row)
    return [('PS_ObjektkeramikID', object_id), *fields] if fields else []


def arachne_modell3d_fields_from_model_files(model_dir: Path, ars_uuid: str) -> Sequence[Tuple[str, str]]:
    uuid_dir = os.path.join(model_dir, ars_uuid)
    if os.path.isdir(uuid_dir):
        obj_files = glob.glob(f'{uuid_dir}/*_reduziert.obj')
        mtl_files = glob.glob(f'{uuid_dir}/*_reduziert.mtl')
        if obj_files and len(obj_files) == 1 and mtl_files and len(mtl_files) == 1:
            return [
                ('Dateiname', os.path.basename(obj_files[0])),
                ('Dateiformat', 'objmtl'),
                ('DateinameMTL', os.path.basename(mtl_files[0])),
                ('Pfad', os.path.join(FOLDER_REMOTE, ars_uuid)),
            ]
        else:
            raise Exception(f'Missing obj or mtl file for id {ars_uuid}.')
    return []


def query_portal_local_or_remote(ars_uuid: str, portal_cache: Optional[Path]) -> str:

    def fetch_remote(save_to_file=None) -> str:
        response = requests.get(PORTAL_URI_TEMPLATE % (ars_uuid, ars_uuid))
        sleep(0.25)  # Be nice and idle a bit between requests
        if response.status_code != 200:
            raise Exception(f'Invalid return code {response.status_code} for id: {ars_uuid}.')
        if save_to_file:
            with open(save_to_file, mode='w', encoding='utf-8') as file:
                file.write(response.text)
        return response.text

    if portal_cache:
        portal_file = os.path.join(portal_cache, f'{ars_uuid}.json')
        if os.path.exists(portal_file):
            with open(portal_file, mode='r', encoding='utf8') as f:
                return f.read()
        else:
            return fetch_remote(portal_file)
    else:
        return fetch_remote()


def arachne_modell3d_technical_notes_from_portal(ars_uuid: str, portal_cache: Optional[Path]) -> str:
    data = json.loads(query_portal_local_or_remote(ars_uuid, portal_cache))

    data_sensor = data['projects'][0]['measurement_series'][0]['sensors'][0]
    data_setup = data['projects'][0]['measurement_series'][0]['measurements'][0]['measurement_setup']
    sensor_1 = data_sensor['capturing_device']['sensor_type']['value']
    cam_img_h = data_setup['image_height']['value']
    cam_img_w = data_setup['image_width']['value']
    mv_length = int(data_sensor['capturing_device']['measuring_volume_length']['value'])
    mv_width = int(data_sensor['capturing_device']['measuring_volume_width']['value'])
    mv_depth = int(data_sensor['capturing_device']['measuring_volume_depth']['value'])
    point_distance = data_sensor['capturing_device']['theoretical_measuring_point_distance']['value']

    data_mesh = data['projects'][0]['meshes'][0]['mesh_information']
    num_points = data_mesh['num_points']['value']
    num_triangles = data_mesh['num_triangles']['value']
    area_cm = int((data_mesh['area']['value']) / 100)

    sensor_2 = data['projects'][1]['chunks'][0]['sensors'][0]['capturing_device']['name']['value']
    img_h = data['projects'][1]['chunks'][0]['sensors'][0]['calibration']['cal_properties']['image_height']['value']
    img_w = data['projects'][1]['chunks'][0]['sensors'][0]['calibration']['cal_properties']['image_width']['value']

    note = (f"3D Capturing\n"
            f"Capturing device: structured light scanner\n"
            f"Sensor:  {sensor_1}\n"
            f"Camera resolution: {int((cam_img_h * cam_img_w) / 1000000)} MegaPixel\n"
            f"Measuring volume: {mv_length} mm x {mv_width} mm x {mv_depth} mm\n"
            f"Theoretical measuring point distance: {point_distance}\n"
            f"\n"
            f"3D Model\n"
            f"Number of points: {num_points}\n"
            f"Number of triangles: {num_triangles}\n"
            f"Area: {area_cm} cm^2\n"
            f"Scale: 1:1\n"
            f"\n"
            f"Texturing\n"
            f"Capturing device: structure from motion\n"
            f"Sensor: {sensor_2}\n"
            f"Camera resoulution: {int((img_h * img_w) / 1000000)} MegaPixel")
    return note


def arachne_modell3d_fields(row: dict, object_id, model_dir: Path, portal_cache: Optional[Path] = None)\
        -> Sequence[Tuple[str, str]]:
    ars_uuid = row.get('object')
    fields = arachne_modell3d_fields_from_model_files(model_dir, ars_uuid)
    if fields:
        technical_note = arachne_modell3d_technical_notes_from_portal(ars_uuid, portal_cache)
        return [
            ('FS_ObjektID', object_id),
            ('Titel', row.get('objectLabel').capitalize()),
            ('Modellierer', CREATOR_NOTE),
            ('TechnischeHinweise', technical_note),
            ('Lizenz', COPYRIGHT),
            ('ModellTyp', 'object'),
            *fields
        ]
    else:
        return []


def insert_stmt_with_params(table: str, fields: Sequence[Tuple[str, str]]):
    field_names = [k for k, _ in fields]
    field_values = [v for _, v in fields]
    placeholders = ['%s' for _ in fields]
    sql = f'INSERT INTO {table} ({", ".join(field_names)}) VALUES ({", ".join(placeholders)})'
    return sql, field_values


def insert(conn: ConnectionWithDryRun, table: str, fields: Sequence[Tuple[str, str]]) -> Optional[int]:
    if fields:
        sql, params = insert_stmt_with_params(table, fields)
        return conn.insert(sql, params)
    else:
        return None


def main(args: argparse.Namespace):
    reader = csv.DictReader(args.objects_csv, delimiter=';')
    with connect(option_files=args.db_config) as mysql_connection:
        connection = ConnectionWithDryRun(mysql_connection, args.dry_run)

        for row in reader:
            obj_fields = arache_object_fields(row)
            obj_id = insert(connection, 'objekt', obj_fields)
            insert(connection, 'ortsbezug', arachne_place_ref_fields(row, obj_id))
            insert(connection, 'datierung', arachne_datierung_fields(row, obj_id))
            insert(connection, 'datierung', arachne_datierung_period_fields(row, obj_id))
            insert(connection, 'objektkeramik', arachne_objektkeramik_fields(row, obj_id))
            insert(connection, 'URI', arachne_uri_fields(row, obj_id))
            insert(connection, 'modell3d', arachne_modell3d_fields(row, obj_id, args.model_dir, args.portal_dir))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('objects_csv', type=argparse.FileType(mode='r'),
                        help='CSV file with object data from the ARS3D portal')
    parser.add_argument('--dry-run', action='store_true', help='If true SQL is output instead of executed.')
    parser.add_argument('--db-config', default=DEFAULT_DB_CONF, type=str,
                        help='An ini-style config file to use for the db connection. '
                             'Expects at least host, database, user, password below a [client] header.')
    parser.add_argument('--model-dir', type=Path, help='Directory to check for 3D model files.',
                        default='/media/archaeocloud/S-Arachne/arachne4scans/arachne4webmodels3d/ars3d-test')
    parser.add_argument('--portal-dir', type=Path,
                        help='Optional: A directory with <id>.json files to use as a cache for querying the portal.')

    main(parser.parse_args())
