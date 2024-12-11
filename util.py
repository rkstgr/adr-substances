import pandas as pd
import tabula


def load_tables_from_pdf(pdf_path):
    return tabula.read_pdf(
        pdf_path,
        pages="all",
        multiple_tables=True,
        lattice=True,
        guess=False,
        pandas_options={"header": None},
    )


def clean_string_content(df):
    """
    Clean string content in DataFrame:
    - Replace "-\r" with ""
    - Replace remaining "\r" with space

    Args:
        df (pandas.DataFrame): Input DataFrame
    Returns:
        pandas.DataFrame: DataFrame with cleaned strings
    """

    def clean_text(text):
        if pd.isna(text):
            return "-"
        if isinstance(text, str):
            # First replace "-\r" with empty string
            text = text.replace("-\r", "")
            # Then replace remaining \r with space
            text = text.replace("\r", " ")
            # Remove any double spaces
            text = " ".join(text.split())
            return text
        return text

    return df.map(clean_text)


def fix_table_1(df):
    # Create a copy of the DataFrame
    df = df.copy()

    if not str(df.loc[0][0]).startswith("UN"):
        df = df.drop(columns=[df.columns[0]])

    # delete the first column, only holds index number
    # df = df.drop(columns=[df.columns[0]])

    # Get the first 4 rows that contain header information
    header_rows = df.iloc[:4]
    content_rows = df.iloc[4:]

    # Create main column names (level 0)
    main_columns = [
        "UN-Nummer",
        "Benennung und Beschreibung",
        "Klasse",
        "Klassifizierungscode",
        "Verpackungsgruppe",
        "Gefahrzettel",
        "Sondervorschriften",
        "Begrenzte und freigestellte Mengen A",
        "Begrenzte und freigestellte Mengen B",
        "Verpackung_Anweisungen",
        "Verpackung_Sondervorschriften",
        "Verpackung_Zusammenpackung",
        "Tank_Anweisungen",
        "Tank_Sondervorschriften",
    ]

    # Create reference numbers (level 1)
    ref_numbers = header_rows.iloc[3].tolist()

    # Create MultiIndex
    try:
        multi_idx = pd.MultiIndex.from_arrays(
            [main_columns, ref_numbers], names=["Bezeichnung", "Spalte"]
        )
    except ValueError:
        print("Main columns", len(main_columns), main_columns)
        print("Spalte", len(ref_numbers), ref_numbers)

    # Create new DataFrame with proper headers
    new_df = content_rows.reset_index(drop=True)  # Get data rows
    new_df.columns = multi_idx  # Set the multi-index columns

    new_df = clean_string_content(new_df)

    # if column Spalte = (4) contains 'verboten', mark all column from (4) (including) with "Beförderung verboten"
    # Handle "Beförderung verboten" cases
    for idx in new_df.index:
        # Check if col (4) contains 'verboten'
        verpackungsgruppe_col = ("Verpackungsgruppe", "(4)")
        if "verboten" in str(new_df.at[idx, verpackungsgruppe_col]).lower():
            # Get the position of column (4) in the DataFrame
            start_idx = new_df.columns.get_loc(verpackungsgruppe_col)
            # Fill all columns from this position onwards
            for col in new_df.columns[start_idx:]:
                new_df.at[idx, col] = "BEFÖRDERUNG VERBOTEN"

    return new_df


def fix_table_2(df):
    """
    Fix the structure of the second page table
    """
    # Create a copy of the DataFrame
    df = df.copy()

    # if the last column contains only nans drop it
    if df.iloc[:, -1].isna().all():
        # Drop the last column
        df = df.drop(columns=[df.columns[-1]])

    # Get the first 4 rows that contain header information
    header_rows = df.iloc[:4]
    content_rows = df.iloc[4:]

    # Create main column names (level 0)
    main_columns = [
        "ADR-Tanks_Tankcodierung",
        "ADR-Tanks_Sondervorschriften",
        "Fahrzeug_Befoerderung_Tanks",
        "Befoerderungskategorie",
        "Sondervorschriften_Versandstuecke",
        "Sondervorschriften_SchuettGut",
        "Sondervorschriften_Handhabung",
        "Sondervorschriften_Betrieb",
        "Nummer_Gefahr",
        "UN-Nummer_2",
        "Benennung_2",
    ]

    # Create reference numbers (level 1)
    ref_numbers = header_rows.iloc[3].tolist()

    # Create MultiIndex
    multi_idx = pd.MultiIndex.from_arrays(
        [
            main_columns,
            ref_numbers,
        ],
        names=["Bezeichnung", "Spalte"],
    )

    # Create new DataFrame with proper headers
    new_df = content_rows.reset_index(drop=True)  # Get data rows
    new_df.columns = multi_idx  # Set the multi-index columns

    # Clean string content
    new_df = clean_string_content(new_df)

    # if column (12) [Spalte] contains verboten, then the row should have in (12) - (20) "Beförderung verboten", in (1) what was previously in (13), and (2) what was previously in (14)
    # Handle "Beförderung verboten" cases
    for idx in new_df.index:
        # Check if col (12) contains 'verboten'
        tank_col = ("ADR-Tanks_Tankcodierung", "(12)")
        if "verboten" in str(new_df.at[idx, tank_col]).lower():
            # Store values that need to be moved
            col_13_value = new_df.at[idx, ("ADR-Tanks_Sondervorschriften", "(13)")]
            col_14_value = new_df.at[idx, ("Fahrzeug_Befoerderung_Tanks", "(14)")]

            # Get the position of column (12) in the DataFrame
            start_idx = new_df.columns.get_loc(tank_col)
            end_idx = new_df.columns.get_loc(("Nummer_Gefahr", "(20)"))

            # Fill columns (12) to (20) with "BEFÖRDERUNG VERBOTEN"
            for col in new_df.columns[start_idx : end_idx + 1]:
                new_df.at[idx, col] = "BEFÖRDERUNG VERBOTEN"

            # Move values to columns (1) and (2)
            new_df.at[idx, ("UN-Nummer_2", "(1)")] = col_13_value
            new_df.at[idx, ("Benennung_2", "(2)")] = col_14_value

    return new_df


def merge_1_2(df1, df2):
    """
    Merge the left and right side tables, removing duplicate identification columns

    Args:
        df1 (pandas.DataFrame): Left side table (from fix_table_1)
        df2 (pandas.DataFrame): Right side table (from fix_table_2)

    Returns:
        pandas.DataFrame: Combined table
    """
    # Remove the last two columns from df2 (UN-Nummer_2 and Benennung_2)
    df2 = df2.drop(columns=[df2.columns[-2], df2.columns[-1]])

    # Merge the DataFrames side by side
    result = pd.concat([df1, df2], axis=1)

    return result


def display_row(df_row):
    """
    Display a single row from the ADR table in a readable format

    Args:
        df_row: DataFrame row (can be from find_by_un result)
    """
    if isinstance(df_row, pd.DataFrame):
        if len(df_row) != 1:
            print(f"Warning: Found {len(df_row)} entries, showing first one")
        row_dict = df_row.iloc[0].to_dict()
    else:
        row_dict = df_row

    # Initialize sections for better organization
    sections = {
        "Identification": [("UN-Nummer", "(1)"), ("Benennung und Beschreibung", "(2)")],
        "Classification": [
            ("Klasse", "(3a)"),
            ("Klassifizierungscode", "(3b)"),
            ("Verpackungsgruppe", "(4)"),
            ("Gefahrzettel", "(5)"),
            ("Sondervorschriften", "(6)"),
        ],
        "Quantities": [
            ("Begrenzte und freigestellte Mengen A", "(7a)"),
            ("Begrenzte und freigestellte Mengen B", "(7b)"),
        ],
        "Packaging": [
            ("Verpackung_Anweisungen", "(8)"),
            ("Verpackung_Sondervorschriften", "(9a)"),
            ("Verpackung_Zusammenpackung", "(9b)"),
        ],
        "Tank": [
            ("Tank_Anweisungen", "(10)"),
            ("Tank_Sondervorschriften", "(11)"),
            ("ADR-Tanks_Tankcodierung", "(12)"),
            ("ADR-Tanks_Sondervorschriften", "(13)"),
        ],
        "Transport": [
            ("Fahrzeug_Befoerderung_Tanks", "(14)"),
            ("Befoerderungskategorie", "(15)"),
            ("Nummer_Gefahr", "(20)"),
        ],
        "Special Provisions": [
            ("Sondervorschriften_Versandstuecke", "(16)"),
            ("Sondervorschriften_SchuettGut", "(17)"),
            ("Sondervorschriften_Handhabung", "(18)"),
            ("Sondervorschriften_Betrieb", "(19)"),
        ],
    }

    # Check if transport is forbidden
    is_forbidden = any(
        "BEFÖRDERUNG VERBOTEN" in str(value) for value in row_dict.values()
    )

    if is_forbidden:
        print("=" * 80)
        print(
            f"UN {row_dict[('UN-Nummer', '(1)')]} - {row_dict[('Benennung und Beschreibung', '(2)')]}"
        )
        print("BEFÖRDERUNG VERBOTEN")
        print("=" * 80)
        return

    # Print sections
    print("=" * 80)
    for section, fields in sections.items():
        print(f"\n{section}:")
        print("-" * 40)
        for field, col_num in fields:
            value = row_dict.get((field, col_num), "")
            if value:  # Only print if there's a value
                print(f"{field.ljust(35)}: {value}")
    print("=" * 80)


def find_by_un(df, un_number):
    """
    Find rows in the DataFrame matching a specific UN number

    Args:
        df (pandas.DataFrame): The processed DataFrame
        un_number (str or int): UN number to search for (e.g., "0004" or 4)

    Returns:
        pandas.DataFrame: Matching rows
    """
    # Convert UN number to string and ensure 4-digit format
    un_str = str(un_number).zfill(4)

    # Get the UN-Nummer column and find matching rows
    # Using .str.strip() to handle any potential whitespace
    matches = df[df[("UN-Nummer", "(1)")].str.strip() == un_str]

    if len(matches) == 0:
        print(f"No entries found for UN number {un_str}")
    else:
        print(f"Found {len(matches)} entries for UN number {un_str}")

    return matches
