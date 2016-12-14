import sys, os, argparse, re

fpkm_re = re.compile("FPKM \"([\d\.]+)\"")
tpm_re = re.compile("TPM \"([\d\.]+)\"")

def main(args):
    files = [("../data/ERR188044_chrX_half1.gtf", 1751527),
             ("../data/ERR188044_chrX_half2.gtf", 772507)]
    gtfs = [(open(f).readlines(), s) for f, s in files]
    print "\n".join(merge_gtfs(gtfs))
    

def merge_gtfs(gtfs):
    
    total_size = sum([size for lines, size in gtfs])
    
    gtf_lines = list()
    fpkms = list()
    rpks = list()

    #
    for lines, size in gtfs:

        rpk_total = 0.0
        gtf_lines.append(list())
        fpkms.append(list())

        for line in lines:
            if line[0] == "#":
                continue

            gtf_lines[-1].append(line.strip())

            fpkm_m = fpkm_re.search(line)

            if fpkm_m:
                new_fpkm = (float(fpkm_m.group(1)) * size) / total_size
                fpkms[-1].append(new_fpkm)
        
        rpks.append(sum(fpkms[-1]) * (total_size)) #Take away the million?
    
    out_lines = list() 

    rpk_total = sum(rpks)
    f = 0
    for file_lines in gtf_lines: 
        i = 0 
        for line in file_lines:
            
            tpm_m = tpm_re.search(line)
            if tpm_m:
                new_tpm = float(tpm_m.group(1))*rpks[f] / rpk_total
                line = tpm_re.sub('TPM "%.6f"' % new_tpm, line)
                line = fpkm_re.sub('FPKM "%.6f"' % fpkms[f][i], line)
                i += 1

            out_lines.append(line)


        f += 1


    return out_lines


if __name__ == "__main__":
    main(sys.argv[1:])

