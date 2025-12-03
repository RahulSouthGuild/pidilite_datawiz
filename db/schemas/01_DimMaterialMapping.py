"""
DimMaterialMapping Table Definition
Stores the mapping of material codes to their final classifications
"""

TABLE = {
    "order": 1,
    "name": "dim_material_mapping",
    "schema": """CREATE TABLE dim_material_mapping (
    brand VARCHAR(118),
    final_classification VARCHAR(118),
    material_code VARCHAR(118),
    parent_division_code VARCHAR(118),
    parent_division_name VARCHAR(118),
    product_group_1_description VARCHAR(140),
    product_group_2_description VARCHAR(140),
    product_group_3_description VARCHAR(140),
    vertical VARCHAR(50),
    material_code_sg_key VARCHAR(118)
)
DISTRIBUTED BY HASH(material_code) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
            """,
    "comments": {
        "table": "Stores the mapping of material codes to their final classifications.",
        "columns": {
            "material_code": "Unique code for the material. This field identifies each record distinctly.",
            "final_classification": "Final classification of the material.",
        },
    },
    "indexes": {},
}
# Note: Seed data mapping is defined in db/seeds/SEED_MAPPING.py
# Schema definitions should not reference data files (separation of concerns)
