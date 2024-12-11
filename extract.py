import pandas as pd
from util import fix_table_1, fix_table_2, merge_1_2, load_tables_from_pdf


def process_pdf_tables(pdf_path):
    """
    Process all tables from a PDF, combining even/odd pairs and concatenating results

    Args:
        pdf_path (str): Path to the PDF file
    Returns:
        pandas.DataFrame: Final concatenated DataFrame of all processed table pairs
    """
    # Extract all tables from PDF
    tables = load_tables_from_pdf(pdf_path)

    # List to store processed and merged table pairs
    processed_pairs = []

    # Process tables in pairs (even, odd)
    for i in range(0, len(tables), 2):
        try:
            # Process even-numbered table (left side)
            left_table = fix_table_1(tables[i])

            # Check if there's a corresponding odd-numbered table
            if i + 1 < len(tables):
                # Process odd-numbered table (right side)
                right_table = fix_table_2(tables[i + 1])

                # Merge the pair
                merged_pair = merge_1_2(left_table, right_table)
                processed_pairs.append(merged_pair)
            else:
                print(f"Warning: Table {i} has no corresponding right side table")
                # Optionally, you could still append the left table alone
                # processed_pairs.append(left_table)

        except Exception as e:
            print(f"Error processing tables {i} and {i+1}: {str(e)}")
            continue

    if not processed_pairs:
        raise ValueError("No tables were successfully processed")

    # Concatenate all processed pairs vertically
    final_df = pd.concat(processed_pairs, axis=0, ignore_index=True)

    return final_df


def main():
    try:
        # Process the PDF and get final DataFrame
        pdf_path = "ADR2023_Substances.pdf"  # Update with your PDF path
        final_df = process_pdf_tables(pdf_path)

        print("Processing completed successfully!")
        print(f"Total rows in final dataset: {len(final_df)}")

        # Display first few rows
        print("\nFirst few rows of the processed data:")
        print(final_df.head())

        final_df.to_excel("ADR2023_Substances.xlsx")
        final_df.to_csv("ADR2023_Substances.csv", sep=";", index=False)

    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
