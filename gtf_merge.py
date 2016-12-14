import sys, os, argparse, re

fpkm_re = re.compile("FPKM \"([\d\.]+)\"")
tpm_re = re.compile("TPM \"([\d\.]+)\"")

def main(args):
    global PRINT_DEBUG

    parser = argparse.ArgumentParser(description="")
    #parser.add_argument('in_fname', default=None, nargs='?', type=str, help="Input file, or stdin if not included.")
    parser.add_argument('-o', dest='out_fname', type=str, help="Output file, or stdout if not included.")
    parser.add_argument('-d', dest='debug', action='store_true', help="Print debug messages to stderr (if -O not also included)")
    #parser.add_argument('-p', dest='out_prefix', default='./', type=str, help="Output file prefix. Will create directories if needed.")
    #parser.add_argument('-x', dest='x', default=1, type=int, help="")

    args = parser.parse_args(args)

    PRINT_DEBUG = args.debug
    #infile = sys.stdin if not args.in_fname else open(args.in_fname, 'r')
    outfile = sys.stdout if not args.out_fname else open(args.out_fname, 'w')
    
    #out_prefix = args.out_prefix
    #if not os.path.exists(os.path.dirname(out_prefix)):
    #    os.makedirs(os.path.dirname(out_prefix))

    files = [("data/ERR188044_chrX_half1.gtf", 1751527),
             ("data/ERR188044_chrX_half2.gtf", 772507)]

    total_size = sum([s for n, s in files])
    
    lines = list()
    fpkms = list()
    rpks = list()

    for filename, size in files:
        f = open(filename)

        rpk_total = 0.0
        lines.append(list())
        fpkms.append(list())

        for line in f:
            if line[0] == "#":
                continue

            lines[-1].append(line)

            fpkm_m = fpkm_re.search(line)

            if fpkm_m:
                new_fpkm = normalize_fpkm(float(fpkm_m.group(1)), size, total_size)
                fpkms[-1].append(new_fpkm)
        
        rpks.append(sum(fpkms[-1]) * (total_size/1000000.0))
    
    rpk_total = sum(rpks)
    f = 0
    for file_lines in lines: 
        i = 0 
        for line in file_lines:
            
            tpm_m = tpm_re.search(line)
            if tpm_m:
                new_tpm = float(tpm_m.group(1))*rpks[f] / rpk_total
                line = tpm_re.sub('TPM "%.6f"' % new_tpm, line)
                line = fpkm_re.sub('FPKM "%.6f"' % fpkms[f][i], line)
                i += 1

            outfile.write(line)


        f += 1


    outfile.close()

def normalize_fpkm(fpkm, group_size, total_size):
    return (fpkm * group_size) / total_size

if __name__ == "__main__":
    main(sys.argv[1:])

