"""
WSProducer.py
Workspace producers using coffea.
"""
from coffea.hist import Hist, Bin, export1d, plot2d
from coffea.processor import ProcessorABC, LazyDataFrame, dict_accumulator
from uproot3 import recreate
import numpy as np
import awkward as ak 
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

        # ##-- 2d histograms 
        # self._accumulator = dict_accumulator({
        #     name: Hist('Events', Bin(name = axes['xaxis']['label'], **axes['xaxis']), Bin(name = axes['yaxis']['label'], **axes['yaxis'])) ##-- Make it 2d by specifying two Binnings 
        #     for name, axes in ((self.naming_schema(hist['name'], region), hist['axes'])
        #                        for _, hist in list(self.histograms.items())
        #                        for region in hist['region'])
        # })        

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

                selectedValues = np.hstack(ak.to_list(df[hist['target']][selec])).flatten()

                output[name].fill(**{
                    # 'weight': weight[selec],
                    name: selectedValues
                })

                del selectedValues   

        # ##-- 2d histograms 
        # for h, hist in list(self.histograms.items()):
        #     for region in hist['region']:

        #         name = self.naming_schema(hist['name'], region)
        #         selec = self.passbut(df, hist['target_x'], region) ##-- Should the selection depend on target?
        #         # selec = self.passbut(df, region)

        #         xax_lab = hist['target_x']
        #         yax_lab = hist['target_y']

        #         xVals = np.hstack(ak.to_list(df[hist['target_x']][selec])).flatten()
        #         yVals = np.hstack(ak.to_list(df[hist['target_y']][selec])).flatten()

        #         output[name].fill(**{
        #             xax_lab : xVals,
        #             yax_lab : yVals 
        #         }
        #         )

        #         del xVals
        #         del yVals 
        #         del name 
        #         del selec 
        #         del xax_lab
        #         del yax_lab

        return output

    def postprocess(self, accumulator):
        return accumulator

    def passbut(self, event: LazyDataFrame, excut: str, cat: str):
        """Backwards-compatible passbut."""
        return eval('&'.join('(' + cut.format(sys=('' if self.weight_syst else self.syst_suffix)) + ')' for cut in self.selection[cat] ))#if excut not in cut))

class SUEP_NTuple(WSProducer):
    histograms = {

        ##-- 1d histograms 
        'time': {
            'target': 'time',
            'name': 'time', 
            # 'region' : ['sevzero_all_', 'sevzero_MostlyZeroed',
            #             'sevthree_all', 'sevthree_MostlyZeroed',
            #             'sevfour_all', 'sevfour_MostlyZeroed'
            #            ],

            'region' : [
                        'sevzero_all_twrADC1to2', 'sevzero_MostlyZeroed_twrADC1to2',
                        'sevzero_all_twrADC2to32', 'sevzero_MostlyZeroed_twrADC2to32',
                        'sevzero_all_twrADC32to256', 'sevzero_MostlyZeroed_twrADC32to256',

                        'sevthree_all_twrADC1to2', 'sevthree_MostlyZeroed_twrADC1to2',
                        'sevthree_all_twrADC2to32', 'sevthree_MostlyZeroed_twrADC2to32',
                        'sevthree_all_twrADC32to256', 'sevthree_MostlyZeroed_twrADC32to256',

                        'sevfour_all_twrADC1to2', 'sevfour_MostlyZeroed_twrADC1to2',
                        'sevfour_all_twrADC2to32', 'sevfour_MostlyZeroed_twrADC2to32',
                        'sevfour_all_twrADC32to256', 'sevfour_MostlyZeroed_twrADC32to256',                                                

                       ],

            # 'region': ['sevall_all', 'sevall_MostlyZeroed', 'sevzero_all', 'sevthree_all', 'sevfour_all', 'sevzero_MostlyZeroed', 'sevthree_MostlyZeroed', 'sevfour_MostlyZeroed'],
            # 'region': ['sevzero_all', 'sevfour_all', 'sevzero_MostlyZeroed', 'sevfour_MostlyZeroed'],
            # 'axis': {'label': 'time', 'n_or_arr': 120, 'lo': -225, 'hi': 125}
            # 'axis': {'label': 'time', 'n_or_arr': 120, 'lo': -225, 'hi': 125}
            'axis': {'label': 'time', 'n_or_arr': 100, 'lo': -50, 'hi': 50}
        }, 

        # 'twrADC': {
        #     'target': 'twrADC',
        #     'name': 'twrADC', 
        #     # 'region': ['sevzero_all', 'sevthree_all', 'sevfour_all', 'sevzero_MostlyZeroed', 'sevthree_MostlyZeroed', 'sevfour_MostlyZeroed'],
        #     # 'region': ['sevall_all', 'sevall_MostlyZeroed', 'sevzero_all', 'sevthree_all', 'sevfour_all', 'sevzero_MostlyZeroed', 'sevthree_MostlyZeroed', 'sevfour_MostlyZeroed'],
        #     'region': ['sevzero_all', 'sevthree_all', 'sevfour_all', 'sevzero_MostlyZeroed', 'sevthree_MostlyZeroed', 'sevfour_MostlyZeroed'],
        #     'axis': {'label': 'twrADC', 'n_or_arr': 256, 'lo': 0, 'hi': 256}
        # }, 

        # 'twrEmul3ADC': {
        #     'target': 'twrADC',
        #     'name': 'twrADC', 
        #     # 'region': ['sevzero_all', 'sevthree_all', 'sevfour_all', 'sevzero_MostlyZeroed', 'sevthree_MostlyZeroed', 'sevfour_MostlyZeroed'],
        #     'region': ['sevall_all', 'sevall_MostlyZeroed', 'sevzero_all', 'sevthree_all', 'sevfour_all', 'sevzero_MostlyZeroed', 'sevthree_MostlyZeroed', 'sevfour_MostlyZeroed'],
        #     'axis': {'label': 'twrADC', 'n_or_arr': 256, 'lo': 0, 'hi': 256}
        # },                 

        # 'time': {
        #     'target': 'time',
        #     'name': 'time', 
        #     'region': ['sevzero_all', 'sevthree_all', 'sevfour_all', 'sevzero_MostlyZeroed', 'sevthree_MostlyZeroed', 'sevfour_MostlyZeroed'],
        #     'axes' : {
        #         'xaxis': {'label': 'time', 'n_or_arr': 120, 'lo': -225, 'hi': 125},
        #         'yaxis': {'label': 'time', 'n_or_arr': 120, 'lo': -225, 'hi': 125}
        #     }

        # },

        ##-- 2d histograms 
        # 'EnergyVsTimeOccupancy': {
        #     # 'target': { 'x': 'twrADC', 'y' : 'twrEmul3ADC'},
        #     'target_x' : 'time',
        #     'target_y' : 'twrADC',
        #     'name': 'EnergyVsTimeOccupancy', 
        #     'region' : ['sevzero_all', 'sevzero_MostlyZeroed',
        #                 'sevthree_all', 'sevthree_MostlyZeroed',
        #                 'sevfour_all', 'sevfour_MostlyZeroed'
        #                ],
        #     'axes' : {
        #         'xaxis': {'label': 'time', 'n_or_arr': 100, 'lo': -50, 'hi': 50},
        #         'yaxis': {'label': 'twrADC', 'n_or_arr': 255, 'lo': 1, 'hi': 256}                
        #     }

        # },  

        # 'EBOcc': {
        #     'target_x' : 'iphi',
        #     'target_y' : 'ieta',
        #     'name': 'EBOcc', 
        #     'region' : ['all'],
        #     # 'region' : ['sevzero_all', 'sevzero_MostlyZeroed',
        #     #             'sevthree_all', 'sevthree_MostlyZeroed',
        #     #             'sevfour_all', 'sevfour_MostlyZeroed'
        #     #            ],
        #     'axes' : {
        #         'xaxis': {'label': 'iphi', 'n_or_arr': 80, 'lo': 0, 'hi': 80},
        #         'yaxis': {'label': 'ieta', 'n_or_arr': 36, 'lo': -18, 'hi': 18}                
        #     }

        # },          

        # 'realVsEmu': {
        #     # 'target': { 'x': 'twrADC', 'y' : 'twrEmul3ADC'},
        #     'target_x' : 'twrEmul3ADC',
        #     'target_y' : 'twrADC',
        #     'name': 'realVsEmu', 
        #     'region': ['sevall_all','sevzero_all', 'sevthree_all', 'sevfour_all'],
        #     'axes' : {
        #         # 'xaxis': {'label': 'twrEmul3ADC', 'n_or_arr': 256, 'lo': 0, 'hi': 256},
        #         # 'yaxis': {'label': 'twrADC', 'n_or_arr': 256, 'lo': 0, 'hi': 256}
        #         'xaxis': {'label': 'twrEmul3ADC', 'n_or_arr': 133, 'lo': 0, 'hi': 256},
        #         'yaxis': {'label': 'twrADC', 'n_or_arr': 133, 'lo': 0, 'hi': 256}                
        #     }

        # },  

    }

    selection = {

        ##-- all
        "sevall_all" : [
                "1"
            ],            

        ##-- Sev 0

        ##-- twrADC1to2 (lower is always inclusive, upper is exclusive)
            "sevzero_all_twrADC1to2" : [
                "event.sevlv == 0",
                "(event.twrADC >= 1)",
                "(event.twrADC < 2)"
	    ],  

            "sevzero_MostlyZeroed_twrADC1to2" : [
                "event.sevlv == 0",
                "(event.twrEmul3ADC) < (event.twrADC * 0.1)",
                "(event.twrADC >= 1)",
                "(event.twrADC < 2)"
	    ],   

        ##-- twrADC2to32
            "sevzero_all_twrADC2to32" : [
                "event.sevlv == 0",
                "(event.twrADC >= 2)",
                "(event.twrADC < 32)"
	    ],  

            "sevzero_MostlyZeroed_twrADC2to32" : [
                "event.sevlv == 0",
                "(event.twrEmul3ADC) < (event.twrADC * 0.1)",
                "(event.twrADC >= 2)",
                "(event.twrADC < 32)"
	    ],   

        ##-- twrADC32to256
            "sevzero_all_twrADC32to256" : [
                "event.sevlv == 0",
                "(event.twrADC >= 32)",
                "(event.twrADC < 256)"
	    ],  

            "sevzero_MostlyZeroed_twrADC32to256" : [
                "event.sevlv == 0",
                "(event.twrEmul3ADC) < (event.twrADC * 0.1)",
                "(event.twrADC >= 32)",
                "(event.twrADC < 256)"
	    ],   



        ##-- Sev 3

        ##-- twrADC1to2 (lower is always inclusive, upper is exclusive)
            "sevthree_all_twrADC1to2" : [
                "event.sevlv == 3",
                "(event.twrADC >= 1)",
                "(event.twrADC < 2)"
	    ],  

            "sevthree_MostlyZeroed_twrADC1to2" : [
                "event.sevlv == 3",
                "(event.twrEmul3ADC) < (event.twrADC * 0.1)",
                "(event.twrADC >= 1)",
                "(event.twrADC < 2)"
	    ],   

        ##-- twrADC2to32
            "sevthree_all_twrADC2to32" : [
                "event.sevlv == 3",
                "(event.twrADC >= 2)",
                "(event.twrADC < 32)"
	    ],  

            "sevthree_MostlyZeroed_twrADC2to32" : [
                "event.sevlv == 3",
                "(event.twrEmul3ADC) < (event.twrADC * 0.1)",
                "(event.twrADC >= 2)",
                "(event.twrADC < 32)"
	    ],   

        ##-- twrADC32to256
            "sevthree_all_twrADC32to256" : [
                "event.sevlv == 3",
                "(event.twrADC >= 32)",
                "(event.twrADC < 256)"
	    ],  

            "sevthree_MostlyZeroed_twrADC32to256" : [
                "event.sevlv == 3",
                "(event.twrEmul3ADC) < (event.twrADC * 0.1)",
                "(event.twrADC >= 32)",
                "(event.twrADC < 256)"
	    ],        


        ##-- Sev 4

        ##-- twrADC1to2 (lower is always inclusive, upper is exclusive)
            "sevfour_all_twrADC1to2" : [
                "event.sevlv == 4",
                "(event.twrADC >= 1)",
                "(event.twrADC < 2)"
	    ],  

            "sevfour_MostlyZeroed_twrADC1to2" : [
                "event.sevlv == 4",
                "(event.twrEmul3ADC) < (event.twrADC * 0.1)",
                "(event.twrADC >= 1)",
                "(event.twrADC < 2)"
	    ],   

        ##-- twrADC2to32
            "sevfour_all_twrADC2to32" : [
                "event.sevlv == 4",
                "(event.twrADC >= 2)",
                "(event.twrADC < 32)"
	    ],  

            "sevfour_MostlyZeroed_twrADC2to32" : [
                "event.sevlv == 4",
                "(event.twrEmul3ADC) < (event.twrADC * 0.1)",
                "(event.twrADC >= 2)",
                "(event.twrADC < 32)"
	    ],   

        ##-- twrADC32to256
            "sevfour_all_twrADC32to256" : [
                "event.sevlv == 4",
                "(event.twrADC >= 32)",
                "(event.twrADC < 256)"
	    ],  

            "sevfour_MostlyZeroed_twrADC32to256" : [
                "event.sevlv == 4",
                "(event.twrEmul3ADC) < (event.twrADC * 0.1)",
                "(event.twrADC >= 32)",
                "(event.twrADC < 256)"
	    ],                         

        }

    def weighting(self, event: LazyDataFrame):
        weight = 1.0
        try:
            weight = event.xsecscale
        except:
            return "ERROR: weight branch doesn't exist"
        return weight

    def naming_schema(self, name, region):
        return f'{name}_{region}{self.syst_suffix}'
