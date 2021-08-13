import os
import re
import pickle 
from coffea.processor import run_uproot_job, futures_executor

import uproot3 as uproot
import argparse
from matplotlib.colors import LogNorm
import time

"""Start singularity"""
# singularity shell -B ${PWD} -B /afs -B /eos /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest

##-- Single file 
# python3 condor_SUEP_WS.py --era=2018 --inDir="/afs/cern.ch/work/a/atishelm/private/CMS-ECAL-Trigger-Group/CMSSW_11_3_0/src/ECALDoubleWeights/ETTAnalyzer/SingleFile/" --treename="ETTAnalyzerTree" --outDir="/eos/user/a/atishelm/www/EcalL1Optimization/ZeroBias_singleFile/" --condor="0"

##-- All blocks 
# python3 condor_SUEP_WS.py --era=2018 --inDir="/eos/cms/store/group/dpg_ecal/alca_ecalcalib/Trigger/DoubleWeights/ZeroBias_2018_EBOnly/ETTAnalyzer_CMSSW_11_3_0_StripZeroing_EBOnly/210626_062710/" --treename="ETTAnalyzerTree" --outDir="/eos/user/a/atishelm/www/EcalL1Optimization/ZeroBias_allBlocks/"
# python3 condor_SUEP_WS.py --era=2018 --inDir="/eos/cms/store/group/dpg_ecal/alca_ecalcalib/Trigger/DoubleWeights/ZeroBias_2018/ETTAnalyzer_CMSSW_11_3_0_StripZeroing/210625_062523/" --treename="ETTAnalyzerTree" --outDir="/eos/user/a/atishelm/www/EcalL1Optimization/ZeroBias_allBlocks/"
# python3 condor_SUEP_WS.py --era=2018 --inDir="/eos/user/a/atishelm/ntuples/EcalL1Optimization/ETTAnalyzer/ZeroBias/ETTAnalyzer_CMSSW_11_3_0/210622_190129/0000/" --treename="ETTAnalyzerTree"
# python3 condor_SUEP_WS.py --era=2018 --inDir="/eos/user/a/atishelm/ntuples/EcalL1Optimization/ETTAnalyzer/ZeroBias/ETTAnalyzer_CMSSW_11_3_0/210622_190129/SingleFile/" --treename="ETTAnalyzerTree"

parser = argparse.ArgumentParser("")
# parser.add_argument('--isMC', type=int, default=1, help="")
parser.add_argument('--jobNum', type=int, default=1, help="")
parser.add_argument('--era', type=str, default="2018", help="")
parser.add_argument('--doSyst', type=int, default=1, help="")
parser.add_argument('--infile', type=str, default="", help="")
parser.add_argument('--dataset', type=str, default="X", help="")
parser.add_argument('--nevt', type=str, default=-1, help="")
parser.add_argument('--treename', type=str, default="Events", help="")
parser.add_argument('--inDir', type=str, default="", help="") ##-- Comma separated list of directories
parser.add_argument('--outDir', type=str, default="./", help="") ##-- Comma separated list of directories
parser.add_argument('--condor', type=int, default=1, help="") 

options = parser.parse_args()

if(options.condor):
    ##-- Condor
    from SUEP_Producer import * 
    from SumWeights import *

else:
    ##-- Locally 
    from python.SUEP_Producer import *
    from python.SumWeights import *

# def inputfile(nanofile):
#     tested = False
#     forceaaa = False
#     pfn = os.popen("edmFileUtil -d %s" % (nanofile)).read()
#     pfn = re.sub("\n", "", pfn)
#     print((nanofile, " -> ", pfn))
#     if (os.getenv("GLIDECLIENT_Group", "") != "overflow" and
#             os.getenv("GLIDECLIENT_Group", "") != "overflow_conservative" and not
#             forceaaa):
#         if not tested:
#             print("Testing file open")
#             testfile = uproot.open(pfn)
#             if testfile:
#                 print("Test OK")
#                 nanofile = pfn
#             else:
#                 if "root://cms-xrd-global.cern.ch/" not in nanofile:
#                     nanofile = "root://cms-xrd-global.cern.ch/" + nanofile
#                 forceaaa = True
#         else:
#             nanofile = pfn
#     else:
#         if "root://cms-xrd-global.cern.ch/" not in nanofile:
#             nanofile = "root://cms-xrd-global.cern.ch/" + nanofile
#     return nanofile


options.dataset='QCD'

pre_selection = ""

if float(options.nevt) > 0:
    print((" passing this cut and : ", options.nevt))
    pre_selection += ' && (Entry$ < {})'.format(options.nevt)

pro_syst = []
ext_syst = []

modules_era = []

# modules_era.append(SUEP_NTuple(isMC=options.isMC, era=int(options.era), do_syst=1, syst_var='', sample=options.dataset,
#                          haddFileName="tree_%s.root" % str(options.jobNum)))

modules_era.append(SUEP_NTuple(era=int(options.era), do_syst=1, syst_var='', sample=options.dataset,
                         haddFileName="tree_%s.root" % str(options.jobNum)))                         

for i in modules_era:
    print("modules : ", i)

if(options.infile != ""):
    if(options.condor):
        filesToRun = [options.infile.split('/')[-1]] ##-- For condor 
    else:
        filesToRun = [options.infile]
else:
    filesToRun = []
    print("Finding files...")
    for root, dirs, files in os.walk(options.inDir, topdown=False):
       for name in files:
          if(name.endswith(".root")):
            # print("found file:",os.path.join(root, name))
            filesToRun.append(os.path.join(root, name))    
    print("Number of input files:",len(filesToRun))

    # files = ["%s/%s"%(options.inDir,f) for f in os.listdir(options.inDir) if f.endswith(".root")]

f = uproot.recreate("ETT_histograms_%s.root" % str(options.jobNum))

##-- Create output directory 
ol = options.outDir
if(not os.path.isdir(ol)):
    print("Output directory '%s' does not exist"%(ol))
    print("Creating output directory '%s'..."%(ol))
    os.system("mkdir -p %s"%(ol))

    ##-- Assuming index.php file exists in previous directory for website
    print("Copying php index '%s/../index.php' to '%s' (for websites only)..."%(ol, ol))
    os.system("cp %s/../index.php %s"%(ol, ol))

for instance in modules_era:

    output = run_uproot_job(
        {instance.sample: filesToRun},
        treename=options.treename,
        processor_instance=instance,
        executor=futures_executor,
        executor_args={  'workers' : 10, ##-- 4510?, 3?
                         'retries' : 2,
                    #    'savemetrics' : True,  
                      },
        chunksize=250000        
    )

    ##-- 1d histograms 
    for h, hist in output.items():
        f[h] = export1d(hist) ##-- for 1d histogram exporting to output root file 
        print(f'wrote {h} to ETT_histograms_{options.jobNum}.root')

    ##-- 2d histograms 

    ##-- https://coffeateam.github.io/coffea/notebooks/histograms.html
    ##-- http://github.com/CoffeaTeam/coffea/blob/515877fa55e914ef82f51f686d1a0eaa9f0f71d1/coffea/hist/hist_tools.py#L903

    # print("savemetrics:")
    # print("output[1]:",output[1])
    
    ##-- Binning for different 2d histogram types 

        # h_cut_high = hist.integrate("twrADC", slice(32, 256))
        # h_cut_high_vals = h_cut_high.values(sumw2=False)[()]
        # high_energy_yield = np.sum(h_cut_high_vals)        

        # yields = [low_energy_yield, high_energy_yield]
        # print("yields",yields)

        ##-- for emu / real plot, get average of each x slice 
        # if("emuOverRealvstwrADC" in h):
        #     totals = []
        #     sliceValues = []
        #     # averages = []
        #     emuOverReal_bins = range(0, 1200, 25)
        #     emuOverReal_bins = [val/1000. for val in emuOverReal_bins]
            
        #     for twrADC_energy in range(1, 256):
        #         h_slice = hist.integrate("twrADC", slice(twrADC_energy, twrADC_energy + 1))
        #         h_slice_vals = h_slice.values(sumw2=False)[()]
        #         # h_slice_sum = np.sum(h_slice_vals)
        #         print("twrADC:",twrADC_energy)
        #         print("h_slice_vals:",h_slice_vals)
        #         sliceValues.append(h_slice_vals)
                # print("---> integral:",h_slice_sum)
                # average_conts = np.multiply(emuOverReal_bins, h_slice_vals)
                # if(np.sum(h_slice_vals) == 0):
                #     average = -1 
                # else:
                #     average = np.average(emuOverReal_bins, weights=h_slice_vals)
                # totals.append(h_slice_sum)
                # averages.append(average)
            # print("totals:",totals)
            # print("averages:",averages)
            # print("emuOverReal_bins:",emuOverReal_bins)

