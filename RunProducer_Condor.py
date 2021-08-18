##-- Example command: python RunProducer_Condor.py --tag=
##-- example command: python run_SUEPProducer.py --era=2018 --tag=210628_214348
##-- example command: python run_SUEPProducer.py --era=2018 --tag=210809_140837
##-- Thank you: https://research.cs.wisc.edu/htcondor/manual/v8.5/condor_submit.html
# https://github.com/htcondor/htcondor/blob/abbf76f596e935d5f2c2645e439cb3bee2eef9a7/src/condor_starter.V6.1/docker_proc.cpp ##-- Docker/HTCondor under the hood 

import os, sys
import argparse
import logging
import pwd
import subprocess
import shutil
import time
import glob

#!/usr/bin/python

logging.basicConfig(level=logging.DEBUG)

script_TEMPLATE = """#!/bin/bash
##source /cvmfs/cms.cern.ch/cmsset_default.sh

##source /cvmfs/cms.cern.ch/cmsset_default.sh
#source /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest
export SCRAM_ARCH=slc7_amd64_gcc820

# export LD_LIBRARY_PATH_STORED=$LD_LIBRARY_PATH_STORED

# eval `scramv1 runtime -sh`
#eval `scramv1 ru -sh`
echo
echo $_CONDOR_SCRATCH_DIR
cd   $_CONDOR_SCRATCH_DIR
echo
echo "... start job at" `date "+%Y-%m-%d %H:%M:%S"`
echo "----- directory before running:"
ls -lR .
#echo "----- CMSSW BASE, python path, pwd:"
#echo "+ CMSSW_BASE  = $CMSSW_BASE"
echo "+ PYTHON_PATH = $PYTHON_PATH"
echo "+ PWD         = $PWD"

python RunProducer.py --jobNum=$1 --infile=$2 --treename="ETTAnalyzerTree"

#rm temp_$1.root
#echo "----- transfer output to eos :"
#mv tree_$1. {outputdir}
echo "----- directory after running :"
ls -lR .
echo " ------ THE END (everyone dies !) ----- "
"""

condor_TEMPLATE = """
#request_disk          = 1024
request_disk          = 2048
request_memory = 8000
executable            = {jobdir}/script.sh
arguments             = $(ProcId) $(jobid) 
transfer_input_files = {transfer_files}, $(jobid)

# environment = "LD_LIBRARY_PATH_STORED=/eos" 

output                = $(ClusterId).$(ProcId).out
error                 = $(ClusterId).$(ProcId).err
log                   = $(ClusterId).$(ProcId).log
initialdir            = {jobdir}
#environment           = 'vanilla'
#transfer_output_remaps = "EBOcc_sevzero_all_values.p={output_dir}/EBOcc_sevzero_all_values_$(ProcId).p;EBOcc_sevzero_MostlyZeroed_values.p={output_dir}/EBOcc_sevzero_MostlyZeroed_values_$(ProcId).p;EBOcc_sevthree_all_values.p={output_dir}/EBOcc_sevthree_all_values_$(ProcId).p;EBOcc_sevthree_MostlyZeroed_values.p={output_dir}/EBOcc_sevthree_MostlyZeroed_values_$(ProcId).p;EBOcc_sevfour_all_values.p={output_dir}/EBOcc_sevfour_all_values_$(ProcId).p;EBOcc_sevfour_MostlyZeroed_values.p={output_dir}/EBOcc_sevfour_MostlyZeroed_values_$(ProcId).p"                                                    
#transfer_output_remaps = "EnergyVsTimeOccupancy_sevzero_all_yields.p={output_dir}/EnergyVsTimeOccupancy_sevzero_all_yields_$(ProcId).p;EnergyVsTimeOccupancy_sevzero_all_values.p={output_dir}/EnergyVsTimeOccupancy_sevzero_all_values_$(ProcId).p;EnergyVsTimeOccupancy_sevzero_MostlyZeroed_yields.p={output_dir}/EnergyVsTimeOccupancy_sevzero_MostlyZeroed_yields_$(ProcId).p;EnergyVsTimeOccupancy_sevzero_MostlyZeroed_values.p={output_dir}/EnergyVsTimeOccupancy_sevzero_MostlyZeroed_values_$(ProcId).p;EnergyVsTimeOccupancy_sevthree_all_yields.p={output_dir}/EnergyVsTimeOccupancy_sevthree_all_yields_$(ProcId).p;EnergyVsTimeOccupancy_sevthree_all_values.p={output_dir}/EnergyVsTimeOccupancy_sevthree_all_values_$(ProcId).p;EnergyVsTimeOccupancy_sevthree_MostlyZeroed_yields.p={output_dir}/EnergyVsTimeOccupancy_sevthree_MostlyZeroed_yields_$(ProcId).p;EnergyVsTimeOccupancy_sevthree_MostlyZeroed_values.p={output_dir}/EnergyVsTimeOccupancy_sevthree_MostlyZeroed_values_$(ProcId).p;EnergyVsTimeOccupancy_sevfour_all_yields.p={output_dir}/EnergyVsTimeOccupancy_sevfour_all_yields_$(ProcId).p;EnergyVsTimeOccupancy_sevfour_all_values.p={output_dir}/EnergyVsTimeOccupancy_sevfour_all_values_$(ProcId).p;EnergyVsTimeOccupancy_sevfour_MostlyZeroed_yields.p={output_dir}/EnergyVsTimeOccupancy_sevfour_MostlyZeroed_yields_$(ProcId).p;EnergyVsTimeOccupancy_sevfour_MostlyZeroed_values.p={output_dir}/EnergyVsTimeOccupancy_sevfour_MostlyZeroed_values_$(ProcId).p"
#transfer_output_remaps = "EBOcc_all_values.p={output_dir}/EBOcc_all_values_$(ProcId).p"
#transfer_output_remaps = "realVsEmu_sevall_all_values.p={output_dir}/realVsEmu_sevall_all_values_$(ProcId).p;realVsEmu_sevzero_all_values.p={output_dir}/realVsEmu_sevzero_all_values_$(ProcId).p;realVsEmu_sevthree_all_values.p={output_dir}/realVsEmu_sevthree_all_values_$(ProcId).p;realVsEmu_sevfour_all_values.p={output_dir}/realVsEmu_sevfour_all_values_$(ProcId).p"
#transfer_output_remaps = "ETT_histograms_$(ProcId).root={output_dir}/ETT_histograms_$(ProcId).root"
#transfer_output_remaps = "emuOverRealvstwrADC_sevzero_all_values.p={output_dir}/emuOverRealvstwrADC_sevzero_all_values_$(ProcId).p;emuOverRealvstwrADC_sevthree_all_values.p={output_dir}/emuOverRealvstwrADC_sevthree_all_values_$(ProcId).p;emuOverRealvstwrADC_sevfour_all_values.p={output_dir}/emuOverRealvstwrADC_sevfour_all_values_$(ProcId).p;emuOverRealvstwrADC_sevzero_all_sliceValues.p={output_dir}/emuOverRealvstwrADC_sevzero_all_sliceValues_$(ProcId).p;emuOverRealvstwrADC_sevthree_all_sliceValues.p={output_dir}/emuOverRealvstwrADC_sevthree_all_sliceValues_$(ProcId).p;emuOverRealvstwrADC_sevfour_all_sliceValues.p={output_dir}/emuOverRealvstwrADC_sevfour_all_sliceValues_$(ProcId).p"
#transfer_output_remaps = "emuOverRealvstwrADC_sevzero_all_values.p={output_dir}/emuOverRealvstwrADC_sevzero_all_values_$(ProcId).p;emuOverRealvstwrADC_sevthree_all_values.p={output_dir}/emuOverRealvstwrADC_sevthree_all_values_$(ProcId).p;emuOverRealvstwrADC_sevfour_all_values.p={output_dir}/emuOverRealvstwrADC_sevfour_all_values_$(ProcId).p"
#transfer_output_remaps = "oneMinusEmuOverRealvstwrADC_sevzero_all_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevzero_all_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevthree_all_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevthree_all_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevfour_all_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevfour_all_values_$(ProcId).p"

#transfer_output_remaps = "oneMinusEmuOverRealvstwrADC_sevzero_inTime_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevzero_inTime_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevthree_inTime_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevthree_inTime_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevfour_inTime_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevfour_inTime_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevzero_Early_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevzero_Early_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevthree_Early_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevthree_Early_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevfour_Early_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevfour_Early_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevzero_Late_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevzero_Late_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevthree_Late_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevthree_Late_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevfour_Late_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevfour_Late_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevzero_VeryLate_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevzero_VeryLate_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevthree_VeryLate_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevthree_VeryLate_values_$(ProcId).p;oneMinusEmuOverRealvstwrADC_sevfour_VeryLate_values.p={output_dir}/oneMinusEmuOverRealvstwrADC_sevfour_VeryLate_values_$(ProcId).p"
transfer_output_remaps = "oneMinusEmuOverRealvstwrADCCourseBinning_sevzero_inTime_values.p={output_dir}/oneMinusEmuOverRealvstwrADCCourseBinning_sevzero_inTime_values_$(ProcId).p;oneMinusEmuOverRealvstwrADCCourseBinning_sevthree_inTime_values.p={output_dir}/oneMinusEmuOverRealvstwrADCCourseBinning_sevthree_inTime_values_$(ProcId).p;oneMinusEmuOverRealvstwrADCCourseBinning_sevfour_inTime_values.p={output_dir}/oneMinusEmuOverRealvstwrADCCourseBinning_sevfour_inTime_values_$(ProcId).p;oneMinusEmuOverRealvstwrADCCourseBinning_sevzero_Early_values.p={output_dir}/oneMinusEmuOverRealvstwrADCCourseBinning_sevzero_Early_values_$(ProcId).p;oneMinusEmuOverRealvstwrADCCourseBinning_sevthree_Early_values.p={output_dir}/oneMinusEmuOverRealvstwrADCCourseBinning_sevthree_Early_values_$(ProcId).p;oneMinusEmuOverRealvstwrADCCourseBinning_sevfour_Early_values.p={output_dir}/oneMinusEmuOverRealvstwrADCCourseBinning_sevfour_Early_values_$(ProcId).p;oneMinusEmuOverRealvstwrADCCourseBinning_sevzero_Late_values.p={output_dir}/oneMinusEmuOverRealvstwrADCCourseBinning_sevzero_Late_values_$(ProcId).p;oneMinusEmuOverRealvstwrADCCourseBinning_sevthree_Late_values.p={output_dir}/oneMinusEmuOverRealvstwrADCCourseBinning_sevthree_Late_values_$(ProcId).p;oneMinusEmuOverRealvstwrADCCourseBinning_sevfour_Late_values.p={output_dir}/oneMinusEmuOverRealvstwrADCCourseBinning_sevfour_Late_values_$(ProcId).p;oneMinusEmuOverRealvstwrADCCourseBinning_sevzero_VeryLate_values.p={output_dir}/oneMinusEmuOverRealvstwrADCCourseBinning_sevzero_VeryLate_values_$(ProcId).p;oneMinusEmuOverRealvstwrADCCourseBinning_sevthree_VeryLate_values.p={output_dir}/oneMinusEmuOverRealvstwrADCCourseBinning_sevthree_VeryLate_values_$(ProcId).p;oneMinusEmuOverRealvstwrADCCourseBinning_sevfour_VeryLate_values.p={output_dir}/oneMinusEmuOverRealvstwrADCCourseBinning_sevfour_VeryLate_values_$(ProcId).p"

#Requirements = HasSingularity
+JobFlavour           = "{queue}"
#+SingularityImage = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest"

HasDocker = true
universe = docker
docker_image = coffeateam/coffea-dask:latest

queue jobid from {jobdir}/inputfiles.dat
"""

def main():
    parser = argparse.ArgumentParser(description='Famous Submitter')
    parser.add_argument("-t"   , "--tag"   , type=str, default="Exorcism"  , help="production tag", required=True)
    parser.add_argument("-q"   , "--queue" , type=str, default="espresso", help="")
    parser.add_argument("-f"   , "--force" , action="store_true"          , help="recreate files and jobs")
    parser.add_argument("-s"   , "--submit", action="store_true"          , help="submit only")
    parser.add_argument("-dry" , "--dryrun", action="store_true"          , help="running without submission")

    options = parser.parse_args()

    indir = "/eos/cms/store/group/dpg_ecal/alca_ecalcalib/Trigger/DoubleWeights/ZeroBias_2018_EBOnly_FixedOccProportion/ETTAnalyzer_CMSSW_11_3_0_StripZeroing_EBOnly_FixedOccProportion/{}/".format(options.tag)

    for sample in os.listdir(indir):
        if "merged" in sample:
            continue
        if "WS" in sample:
            continue
        if "output" in sample:
            continue 

        jobs_dir = '_'.join(['jobs', options.tag, sample])
        logging.info("-- sample_name : " + sample)

        if os.path.isdir(jobs_dir):
            if not options.force:
                logging.error(" " + jobs_dir + " already exist !")
                continue
            else:
                logging.warning(" " + jobs_dir + " already exists, forcing its deletion!")
                shutil.rmtree(jobs_dir)
                os.mkdir(jobs_dir)
        else:
            os.mkdir(jobs_dir)
        with open(os.path.join(jobs_dir, "inputfiles.dat"), 'w') as infiles:
            in_files = glob.glob("{indir}/{sample}/*.root".format(sample=sample, indir=indir))
            for name in in_files:
                infiles.write(name+"\n")
            infiles.close()
        outdir = indir + sample + "_output/"
        os.system("mkdir -p {}".format(outdir))

        with open(os.path.join(jobs_dir, "script.sh"), "w") as scriptfile:
            script = script_TEMPLATE.format(
                outputdir=outdir
            )
            scriptfile.write(script)
            scriptfile.close()

        with open(os.path.join(jobs_dir, "condor.sub"), "w") as condorfile:
            allFiles = [
                "../RunProducer.py",
                "../python/Producer.py",
                "../python/SumWeights.py"
            ]
            condor = condor_TEMPLATE.format(
                transfer_files = ",".join(allFiles),
                output_dir = outdir,
                jobdir=jobs_dir,
                queue=options.queue
            )
            condorfile.write(condor)
            condorfile.close()
        if options.dryrun:
            continue

        htc = subprocess.Popen(
            "condor_submit " + os.path.join(jobs_dir, "condor.sub"),
            shell  = True,
            stdin  = subprocess.PIPE,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            close_fds=True
        )
        out, err = htc.communicate()
        exit_status = htc.returncode
        logging.info("condor submission status : {}".format(exit_status))

if __name__ == "__main__":
    main()
    print("DONE")
