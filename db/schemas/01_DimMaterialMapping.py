"""
DimMaterialMapping Table Definition
Stores the mapping of material codes to their final classifications
"""

TABLE = {
    "order": 1,
    "name": "DimMaterialMapping",
    "schema": """CREATE TABLE DimMaterialMapping (
    Brand VARCHAR(118),
    FinalClassification VARCHAR(118),
    MaterialCode VARCHAR(118),
    ParentDivisionCode VARCHAR(118),
    ParentDivisionName VARCHAR(118),
    ProductGroup1Description VARCHAR(140),
    ProductGroup2Description VARCHAR(140),
    ProductGroup3Description VARCHAR(140),
    Vertical VARCHAR(50),
    MaterialCodeSGKey VARCHAR(118)
)
DISTRIBUTED BY HASH(MaterialCode) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
            """,
    "comments": {
        "table": "Stores the mapping of material codes to their final classifications.",
        "columns": {
            "MaterialCode": "Unique code for the material. This field identifies each record distinctly.",
            "FinalClassification": "Final classification of the material.",
        },
    },
    "indexes": {},
}
# Note: Seed data mapping is defined in db/seeds/SEED_MAPPING.py
# Schema definitions should not reference data files (separation of concerns)
