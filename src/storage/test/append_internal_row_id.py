import csv
import sys

def add_internal_row_id(input_file, output_file):
    with open(input_file, mode="r", newline="", encoding="utf-8") as infile, \
         open(output_file, mode="w", newline="", encoding="utf-8") as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        header = next(reader)
        writer.writerow(header + ["internal_row_id"])  # Add new column name

        for row_id, row in enumerate(reader, start=1):
            writer.writerow(row + [row_id])

    print(f"✅ Added 'internal_row_id' column and saved to: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python add_row_id.py <input_csv> <output_csv>")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_csv = sys.argv[2]

    add_internal_row_id(input_csv, output_csv)
