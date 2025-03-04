"""
18 August 2021 

The purpose of this module is to run a coffea producer to process root files. 
"""

import os
import re
import pickle 
from coffea.processor import run_uproot_job, futures_executor

import uproot3 as uproot
import argparse
from matplotlib.colors import LogNorm
import time

##-- Single file 
# python3 condor_SUEP_WS.py --era=2018 --inDir="/afs/cern.ch/work/a/atishelm/private/CMS-ECAL-Trigger-Group/CMSSW_11_3_0/src/ECALDoubleWeights/ETTAnalyzer/SingleFile/" --treename="ETTAnalyzerTree" --outDir="/eos/user/a/atishelm/www/EcalL1Optimization/ZeroBias_singleFile/" --condor="0"

##-- All blocks 
# python3 condor_SUEP_WS.py --era=2018 --inDir="/eos/cms/store/group/dpg_ecal/alca_ecalcalib/Trigger/DoubleWeights/ZeroBias_2018_EBOnly/ETTAnalyzer_CMSSW_11_3_0_StripZeroing_EBOnly/210626_062710/" --treename="ETTAnalyzerTree" --outDir="/eos/user/a/atishelm/www/EcalL1Optimization/ZeroBias_allBlocks/"
# python3 condor_SUEP_WS.py --era=2018 --inDir="/eos/cms/store/group/dpg_ecal/alca_ecalcalib/Trigger/DoubleWeights/ZeroBias_2018/ETTAnalyzer_CMSSW_11_3_0_StripZeroing/210625_062523/" --treename="ETTAnalyzerTree" --outDir="/eos/user/a/atishelm/www/EcalL1Optimization/ZeroBias_allBlocks/"
# python3 condor_SUEP_WS.py --era=2018 --inDir="/eos/user/a/atishelm/ntuples/EcalL1Optimization/ETTAnalyzer/ZeroBias/ETTAnalyzer_CMSSW_11_3_0/210622_190129/0000/" --treename="ETTAnalyzerTree"
# python3 condor_SUEP_WS.py --era=2018 --inDir="/eos/user/a/atishelm/ntuples/EcalL1Optimization/ETTAnalyzer/ZeroBias/ETTAnalyzer_CMSSW_11_3_0/210622_190129/SingleFile/" --treename="ETTAnalyzerTree"

parser = argparse.ArgumentParser("")
parser.add_argument('--jobNum', type=int, default=1, help="")
# parser.add_argument('--era', type=str, default="2018", help="")
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
    from Producer import * 
    from SumWeights import *

else:
    ##-- Locally 
    from python.Producer import *
    from python.SumWeights import *

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

modules_era.append(ETT_NTuple(do_syst=1, syst_var='', sample=options.dataset,
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

# f = uproot.recreate("ETT_histograms_%s.root" % str(options.jobNum))

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

    # ##-- 1d histograms 
    # for h, hist in output.items():
    #     f[h] = export1d(hist) ##-- for 1d histogram exporting to output root file 
    #     print(f'wrote {h} to ETT_histograms_{options.jobNum}.root')

    ##-- 2d histograms 

    ##-- https://coffeateam.github.io/coffea/notebooks/histograms.html
    ##-- http://github.com/CoffeaTeam/coffea/blob/515877fa55e914ef82f51f686d1a0eaa9f0f71d1/coffea/hist/hist_tools.py#L903

    # print("savemetrics:")
    # print("output[1]:",output[1])
    
    ##-- Binning for different 2d histogram types 

    twod_plot_labels = ["EnergyVsTimeOccupancy", "EBOcc", "realVsEmu", "emuOverRealvstwrADC", "oneMinusEmuOverRealvstwrADC", "oneMinusEmuOverRealvstwrADCCourseBinning"]

    binDict = {
        "EnergyVsTimeOccupancy" : [[-50, 50], [1, 256]],
        "EBOcc" : [[0, 80], [-18, 18]],
        "realVsEmu" : [[0, 256], [0, 256]],
        "emuOverRealvstwrADC" : [[1, 256], [0, 1.2]],
        "oneMinusEmuOverRealvstwrADC" : [[1, 256], [0, 1.2]],
        # "oneMinusEmuOverRealvstwrADCCourseBinning" :  [[1, 256], [0, 1.2]] 
        # "oneMinusEmuOverRealvstwrADCCourseBinning" :  [[1, 256], [-1, 1.2]] # 88, 'lo': -1, 'hi': 1.2
        # "oneMinusEmuOverRealvstwrADCCourseBinning" :  [[1, 256], [-2, 1.2]] # 88, 'lo': -1, 'hi': 1.2
        "oneMinusEmuOverRealvstwrADCCourseBinning" :  [[1, 256], [-10, 1.2]] # 88, 'lo': -1, 'hi': 1.2
    }

    # for h, hist in output[0].items(): ##-- output[0] if savemetrics is on
    for h, hist in output.items(): ##-- output[0] if savemetrics is on
        print("Saving histogram %s..."%(h))

        ##-- Get yields 
        # h_cut_low = hist.integrate("twrADC", slice(1, 32))
        # h_cut_low_vals = h_cut_low.values(sumw2=False)[()]
        # low_energy_yield = np.sum(h_cut_low_vals)

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

        # print("sliceValues:",sliceValues)

        ##-- 2d histogram processing 
        values = hist.values()[()]
        xaxis = hist.axes()[0]
        histName = "%s_2d"%(h)

        ##-- Pickle yields and values for plots 

        ##-- Condor
        if(options.condor):
            # pickle.dump( yields, open( '%s_yields.p'%(h), "wb" )) ##-- per severity, selection 
            pickle.dump( values, open( '%s_values.p'%(h), "wb" ))  
            # if("emuOverRealvstwrADC" in h):
                # pickle.dump( sliceValues, open( '%s_sliceValues.p'%(h), "wb" ))

        ##-- Locally
        else: 
            # pickle.dump( yields, open( '%s/%s_yields.p'%(ol, histName), "wb" ))
            pickle.dump( values, open( '%s/%s_values.p'%(ol, histName), "wb" ))

            ax = plot2d(
                hist, 
                xaxis = xaxis,
                patch_opts = dict(
                    cmap = 'jet',
                    # cmap = 'Blues', ##-- For real vs. emu TP
                    # vmin = 0
                    # norm = LogNorm(vmin = 1)
                    norm = LogNorm(vmin = 1)
                    )
                )

            for twod_plot_label in twod_plot_labels:
                if(twod_plot_label in h):
                    xLims = binDict[twod_plot_label][0]
                    yLims = binDict[twod_plot_label][1]

                    xmin, xmax = xLims[0], xLims[1]
                    ymin, ymax = yLims[0], yLims[1]

            ax.set_ylim(ymin, ymax)
            ax.set_xlim(xmin, xmax)

            # ax.plot([0, 256], [0, 256], linestyle = '-', color = 'black', linewidth = 0.01) ##-- for real vs emu  
            # ax.hlines([32], xmin = xmin, xmax = xmax, color = 'black') 

            # ax.set_xscale('log')
            fig = ax.figure

            ##-- Pickle the axis for post-processing changes 
            pickle.dump( ax, open( '%s/%s.p'%(ol, histName), "wb" ))
            # if("emuOverRealvstwrADC" in h):
                # pickle.dump( sliceValues, open( '%s/%s_sliceValues.p'%(ol, histName), "wb" ))            

            ##-- Save output plots         
            fig.savefig('%s/%s.png'%(ol, histName))
            fig.savefig('%s/%s.pdf'%(ol, histName))

            print("Saved plot %s/%s.png"%(ol, histName))
            print("Saved plot %s/%s.pdf"%(ol, histName))
