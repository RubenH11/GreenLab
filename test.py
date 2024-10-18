import os

# Define the library structure
libraries = {
    "Pandas": {
        "isna": "pandasDfos.isna()",
        "replace": "pandasDfos.replace('OWN')",
        "groupby": "pandasDfos.groupby(df_col_grade)",
        "sort": "pandasDfos.sort('loan_amnt')",
        "mean": "pandasDfos.mean(df_col_loan_amount)",
        "drop": "pandasDfos.drop('loan_amnt')",
        "dropna": "pandasDfos.dropna()",
        "fillna": "pandasDfos.fillna()",
        "concat": "pandasDfos.concat(df_col_grade)"
    },
    "Modin": {
        "isna": "modinDfos.isna()",
        "replace": "modinDfos.replace('OWN')",
        "groupby": "modinDfos.groupby(mdf_col_grade)",
        "sort": "modinDfos.sort('loan_amnt')",
        "mean": "modinDfos.mean(mdf_col_loan_amount)",
        "drop": "modinDfos.drop('loan_amnt')",
        "dropna": "modinDfos.dropna()",
        "fillna": "modinDfos.fillna()",
        "concat": "modinDfos.concat(mdf_col_grade)"
    },
    "Polars": {
        "isna": "polarsDfos.isna()",
        "replace": "polarsDfos.replace('OWN')",
        "groupby": "polarsDfos.groupby(pdf_col_grade)",
        "sort": "polarsDfos.sort('loan_amnt')",
        "mean": "polarsDfos.mean('loan_amnt')",
        "drop": "polarsDfos.drop('loan_amnt')",
        "dropna": "polarsDfos.dropna()",
        "fillna": "polarsDfos.fillna()",
        "concat": "polarsDfos.concat(pdf_col_grade_as_renamed)"
    },
    "Dask": {
        "isna": "daskDfos.isna()",
        "replace": "daskDfos.replace('OWN')",
        "groupby": "daskDfos.groupby(ddf_col_grade)",
        "sort": "daskDfos.sort('loan_amnt')",
        "mean": "daskDfos.mean(ddf_col_loan_amount)",
        "drop": "daskDfos.drop('loan_amnt')",
        "dropna": "daskDfos.dropna()",
        "fillna": "daskDfos.fillna()",
        "concat": "daskDfos.concat(ddf_col_grade)"
    }
}

# Create the main directory
main_dir = "Libs"
os.makedirs(main_dir, exist_ok=True)

# Create folders and files
for lib, operations in libraries.items():
    lib_dir = os.path.join(main_dir, lib)
    os.makedirs(lib_dir, exist_ok=True)
    
    for op, func in operations.items():
        file_path = os.path.join(lib_dir, f"{op}.py")
        with open(file_path, 'w') as f:
            params = ', '.join(['pandasDfos', 'df_col_grade'] if lib == 'Pandas' and op == 'groupby' else [f"{lib.lower()}Dfos"])
            f.write(f"def f({params}):\n")
            f.write(f"    {func}\n")
