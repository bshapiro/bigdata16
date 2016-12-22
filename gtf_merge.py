import re
import sys

fpkm_re = re.compile("FPKM \"([\d\.]+)\"")
tpm_re = re.compile("TPM \"([\d\.]+)\"")


##############################################################################
# Takes a list of multiple contents of gtf files and number of mapped reads  #
# used to generate each and returns the lines of a merged and normalized GTF #
##############################################################################
def merge_gtfs(gtfs, sizes):

    total_size = sum(sizes)
    fpkms = list()  # stores normalized FPKMs for each transcript

    file_rpks = list()  # stores sum of transcript RPK values (fpkm*size) for each file
    
    #Find and normalize FPKM value for each transcript
    for size, file_lines in zip(sizes, gtfs):
        
        file_fpkm = 0.0 # stores sum of FPKM value for each file

        for line in file_lines:

            #Skip comments
            if line[0] == "#":
                continue
            
            #Check if line has FPKM value
            #Only lines for transcripts should match
            fpkm_m = fpkm_re.search(line)
            if fpkm_m:

                #Calculate new FPKM
                new_fpkm = (float(fpkm_m.group(1)) * size) / total_size
                fpkms.append(new_fpkm)
                file_fpkm += new_fpkm
        
        #Store total RPK value for this file
        file_rpks.append(file_fpkm * total_size)
    
    total_rpk = sum(file_rpks)
    
    #Stores output file lines
    out_lines = list()
    
    #Re-parse file to normalize TPM value (based on RPK)
    f = 0
    r = 0
    for file_lines in gtfs:

        #Value used to normalize TPM
        tpm_factor = file_rpks[r] / total_rpk
        r += 1

        for line in file_lines:

            #Skip comments
            if line[0] == "#":
                continue
            
            #Find TPM value
            tpm_m = tpm_re.search(line)
            if tpm_m:
                new_tpm = float(tpm_m.group(1)) * tpm_factor
                line = tpm_re.sub('TPM "%.6f"' % new_tpm, line)
                line = fpkm_re.sub('FPKM "%.6f"' % fpkms[f], line)
                f += 1
            
            #Store new line
            out_lines.append(line.strip())

    return out_lines

