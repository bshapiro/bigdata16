import re

fpkm_re = re.compile("FPKM \"([\d\.]+)\"")
tpm_re = re.compile("TPM \"([\d\.]+)\"")


def merge_gtfs(gtfs, sizes):
    """
    Takes a list of multiple contents of gtf files and number of mapped reads
    used to generate each and returns the lines of a merged and normalized GTF.
    """

    total_size = sum(sizes)
    fpkms = list()  # stores normalized FPKMs for each transcript

    file_rpks = list()  # stores sum of transcript RPK values (fpkm*size) for each file

    for size, file_lines in zip(sizes, gtfs):  # find and normalize FPKM value for each transcript

        file_fpkm = 0.0  # stores sum of FPKM value for each file

        for line in file_lines:

            if line[0] == "#":  # skip comments
                continue

            fpkm_m = fpkm_re.search(line)  # check if line has FPKM value
            if fpkm_m:
                new_fpkm = (float(fpkm_m.group(1)) * size) / total_size  # calculate normalized FPKM
                fpkms.append(new_fpkm)
                file_fpkm += new_fpkm

        file_rpks.append(file_fpkm * total_size)  # store total RPK value for this file

    total_rpk = sum(file_rpks)
    out_lines = []  # stores output file lines

    f = 0
    r = 0
    for file_lines in gtfs:  # re-parse file to normalize TPM value (based on RPK)

        tpm_factor = file_rpks[r] / total_rpk  # value used to normalize TPM
        r += 1

        for line in file_lines:
            if line[0] == "#":  # skip comments
                continue

            tpm_m = tpm_re.search(line)  # find TPM value
            if tpm_m:
                new_tpm = float(tpm_m.group(1)) * tpm_factor
                line = tpm_re.sub('TPM "%.6f"' % new_tpm, line)
                line = fpkm_re.sub('FPKM "%.6f"' % fpkms[f], line)
                f += 1

            out_lines.append(line.strip())  # store new line

    return out_lines
