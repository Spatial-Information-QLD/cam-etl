import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("input_file")
args = parser.parse_args()

input_file = args.input_file

outfilename = "fixed-" + input_file
with open(input_file, "r", encoding="utf-8") as file:
    with open(outfilename, "w", encoding="utf-8") as outfile:
        for line in file:
            if "\00" in line:
                line = line.replace("\00", "")
            
            outfile.write(line)

Path(input_file).unlink()
Path(outfilename).rename(input_file)
