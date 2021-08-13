"""
WSProducer.py
Workspace producers using coffea.
"""
from coffea.hist import Hist, Bin, export1d, plot2d
from coffea.processor import ProcessorABC, LazyDataFrame, dict_accumulator
from uproot3 import recreate
import numpy as np
import awkward as ak 
import copy 
# from numba import jit

class WSProducer(ProcessorABC):
    """
    A coffea Processor which produces a workspace.
    This applies selections and produces histograms from kinematics.
    """

    histograms = NotImplemented
    selection = NotImplemented

    def __init__(self, era=2017, sample="DY", do_syst=False, syst_var='', weight_syst=False, haddFileName=None, flag=False):
        self._flag = flag
        self.do_syst = do_syst
        self.era = era
        self.sample = sample
        self.syst_var, self.syst_suffix = (syst_var, f'_sys_{syst_var}') if do_syst and syst_var else ('', '')
        self.weight_syst = weight_syst

        ##-- 1d histograms
        self._accumulator = dict_accumulator({
            name: Hist('Events', Bin(name=name, **axis))
            for name, axis in ((self.naming_schema(hist['name'], region), hist['axis'])
                               for _, hist in list(self.histograms.items())
                               for region in hist['region'])
        })

        ###-- 2d histograms 
        #self._accumulator = dict_accumulator({
        #    name: Hist('Events', Bin(name = axes['xaxis']['label'], **axes['xaxis']), Bin(name = axes['yaxis']['label'], **axes['yaxis'])) ##-- Make it 2d by specifying two Binnings 
        #    for name, axes in ((self.naming_schema(hist['name'], region), hist['axes'])
        #                       for _, hist in list(self.histograms.items())
        #                       for region in hist['region'])
        #})        

        self.outfile = haddFileName

    def __repr__(self):
        return f'{self.__class__.__name__}(era: {self.era}, sample: {self.sample}, do_syst: {self.do_syst}, syst_var: {self.syst_var}, weight_syst: {self.weight_syst}, output: {self.outfile})'

    @property
    def accumulator(self):
        return self._accumulator
    
    def process(self, df, *args):
        output = self.accumulator.identity()

        # weight = self.weighting(df)

        ##-- 1d histograms 
        for h, hist in list(self.histograms.items()):
            for region in hist['region']:

                name = self.naming_schema(hist['name'], region)
                selec = self.passbut(df, hist['target'], region)
                
                #selectedValues = np.hstack(ak.to_list(df[hist['target']][selec])).flatten()

                output[name].fill(**{
                    # 'weight': weight[selec],
                    #name: df[hist['target']][selec].flatten()
                    name: df[hist['target']][selec]
                })

                #del selectedValues   

        return output

    def postprocess(self, accumulator):
        return accumulator

    def passbut(self, event: LazyDataFrame, excut: str, cat: str):
        """Backwards-compatible passbut."""
        return eval('&'.join('(' + cut.format(sys=('' if self.weight_syst else self.syst_suffix)) + ')' for cut in self.selection[cat] ))#if excut not in cut))

class SUEP_NTuple(WSProducer):

    #}
    histograms = {

        'Zlep_cand_mass': {
            'target': 'Zlep_cand_mass',
            'name'  : 'Zlep_cand_mass',  # name to write to histogram
            'region': ['signal'],
            'axis': {'label': 'Zlep_cand_mass', 'n_or_arr': 100, 'lo': 50, 'hi': 100}
        },
   } 

    selection = {
            "signal" : [
                "event.ngood_bjets     >  0",
                "event.lep_category    == 2",
                "event.leading_lep_pt  > 25",
                "event.trailing_lep_pt > 20"
            ],
        }


    print("selection:",selection)
    
    def weighting(self, event: LazyDataFrame):
        weight = 1.0
        try:
            weight = event.xsecscale
        except:
            return "ERROR: weight branch doesn't exist"
        return weight

    def naming_schema(self, name, region):
        return f'{name}_{region}{self.syst_suffix}'
