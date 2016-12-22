import re
import sys

fpkm_re = re.compile("FPKM \"([\d\.]+)\"")
tpm_re = re.compile("TPM \"([\d\.]+)\"")
cov_re = re.compile("cov \"([\d\.]+)\"")


def main(args):
    files = ["../data/ERR188044_chrX_half1.gtf", "../data/ERR188044_chrX_half2.gtf"]
    sizes = [1751527, 772507]

    #files = ["test_1.gtf", "test_2.gtf"]
    #sizes = [11995, 82509]
    #sizes = [10908, 78849]
    gtfs = [open(f).readlines() for f in files]
    for line in merge_gtfs(gtfs, sizes):
        print line


def merge_gtfs(gtfs, sizes):
    total_size = sum(sizes)
    fpkms = list()  # stores normalized FPKMs for each transcript

    file_rpks = list()  # stores sum of transcript RPK values (fpkm*size) for each file
    #file_covs = list()

    #total_fpkm = 0.0

    for size, file_lines in zip(sizes, gtfs):
        file_fpkm = 0.0
        file_cov = 0.0
        for line in file_lines:
            if line[0] == "#":
                continue

            fpkm_m = fpkm_re.search(line)
            if fpkm_m:
                #cov_m = cov_re.search(line)
                #file_cov += float(cov_m.group(1))

                new_fpkm = (float(fpkm_m.group(1)) * size) / total_size
                fpkms.append(new_fpkm)
                file_fpkm += new_fpkm
                #total_fpkm += new_fpkm

        file_rpks.append(file_fpkm * total_size)
        #file_covs.append(file_cov)

    total_rpk = sum(file_rpks)

    out_lines = list()

    f = 0
    r = 0
    for file_lines in gtfs:
        tpm_factor = file_rpks[r] / total_rpk
        r += 1

        for line in file_lines:
            if line[0] == "#":
                continue

            tpm_m = tpm_re.search(line)
            if tpm_m:
                new_tpm = float(tpm_m.group(1)) * tpm_factor
                #new_tpm = (1000000*fpkms[f])/total_fpkm
                #new_tpm = float(tpm_m.group(1)) * file_covs[r-1] / sum(file_covs)
                line = tpm_re.sub('TPM "%.6f"' % new_tpm, line)
                line = fpkm_re.sub('FPKM "%.6f"' % fpkms[f], line)
                f += 1

            out_lines.append(line.strip())

    return out_lines


if __name__ == "__main__":
    main(sys.argv[1:])
