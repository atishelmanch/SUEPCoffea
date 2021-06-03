"""
WSProducer.py
Workspace producers using coffea.
"""
from coffea.hist import Hist, Bin, export1d, plot2d
from coffea.processor import ProcessorABC, LazyDataFrame, dict_accumulator
from uproot3 import recreate
import numpy as np
import awkward as ak 

class WSProducer(ProcessorABC):
    """
    A coffea Processor which produces a workspace.
    This applies selections and produces histograms from kinematics.
    """

    histograms = NotImplemented
    selection = NotImplemented

    def __init__(self, isMC, era=2017, sample="DY", do_syst=False, syst_var='', weight_syst=False, haddFileName=None, flag=False):
        self._flag = flag
        self.do_syst = do_syst
        self.era = era
        self.isMC = isMC
        self.sample = sample
        self.syst_var, self.syst_suffix = (syst_var, f'_sys_{syst_var}') if do_syst and syst_var else ('', '')
        self.weight_syst = weight_syst

        # ##-- 1d histograms
        # self._accumulator = dict_accumulator({
        #     name: Hist('Events', Bin(name=name, **axis))
        #     for name, axis in ((self.naming_schema(hist['name'], region), hist['axis'])
        #                        for _, hist in list(self.histograms.items())
        #                        for region in hist['region'])
        # })

        ##-- 2d histograms 
        self._accumulator = dict_accumulator({
            name: Hist('Events', Bin(name = axes['xaxis']['label'], **axes['xaxis']), Bin(name = axes['yaxis']['label'], **axes['yaxis'])) ##-- Make it 2d by specifying two Binnings 
            for name, axes in ((self.naming_schema(hist['name'], region), hist['axes'])
                               for _, hist in list(self.histograms.items())
                               for region in hist['region'])
        })        

        self.outfile = haddFileName

    def __repr__(self):
        return f'{self.__class__.__name__}(era: {self.era}, isMC: {self.isMC}, sample: {self.sample}, do_syst: {self.do_syst}, syst_var: {self.syst_var}, weight_syst: {self.weight_syst}, output: {self.outfile})'

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
                selec = self.passbut(df, hist['target_x'], region) ##-- Should the selection depend on target?
                # selec = self.passbut(df, region)

                # print("values:",df[hist['target']])
                # print("target:",hist['target'])
                # print("region:",region)
                # print("selec:",selec)

                # selectedValues = np.hstack(ak.to_list(df[hist['target']][selec])).flatten()

                # print("x:",hist['target_x'])
                # print("y:",hist['target_y'])
                # print("")

                xax_lab = hist['target_x']
                yax_lab = hist['target_y']

                xVals = np.hstack(ak.to_list(df[hist['target_x']][selec])).flatten()
                yVals = np.hstack(ak.to_list(df[hist['target_y']][selec])).flatten()


                output[name].fill(**{
                    xax_lab : xVals,
                    yax_lab : yVals 
                }
                )

                # output[name].fill(
                    
                #     hist['target_x'] = xVals,
                #     hist['target_y'] = yVals 
                    # x = xVals,
                    # y = yVals                      

                

                # output[name].fill(**{
                    
                #     hist['target_x'] : xVals,
                #     hist['target_y'] : yVals  

                # }
                    # **{
                    # 'weight': weight[selec],
                  
                  
                    # name: selectedValues
                  
                  
                    # name: df[hist['target']][selec]
                    # name: df[hist['target']][selec].array().flatten()
                    # name: df[hist['target']][selec]#.flatten()
                    # name: df[hist['target']][selec].flatten()
                # }
                
                
                # )

        # ##-- 2d histograms 
        # for h, hist in list(self.twoD_histograms.items()):
        #     for region in hist['region']:

        #         name = self.naming_schema(hist['name'], region)
        #         selec = self.passbut(df, hist['target'], region)

        #         selectedValues = np.hstack(ak.to_list(df[hist['target']][selec])).flatten()

        #         output[name].fill(**{
        #             # 'weight': weight[selec],
        #             name: selectedValues
        #             # name: df[hist['target']][selec]
        #             # name: df[hist['target']][selec].array().flatten()
        #             # name: df[hist['target']][selec]#.flatten()
        #             # name: df[hist['target']][selec].flatten()
        #         })            

        return output

    def postprocess(self, accumulator):
        return accumulator

    def passbut(self, event: LazyDataFrame, excut: str, cat: str):
        """Backwards-compatible passbut."""
        return eval('&'.join('(' + cut.format(sys=('' if self.weight_syst else self.syst_suffix)) + ')' for cut in self.selection[cat] ))#if excut not in cut))

class SUEP_NTuple(WSProducer):
    histograms = {

        # 'time': {
        #     'target': 'time',
        #     'name': 'time', 
        #     'region': ['sevzero_all', 'sevthree_all', 'sevfour_all'],
        #     'axes' : {
        #         'xaxis': {'label': 'time', 'n_or_arr': 120, 'lo': -225, 'hi': 125},
        #         'yaxis': {'label': 'time', 'n_or_arr': 120, 'lo': -225, 'hi': 125}
        #     }

        # },

        'twod': {
            # 'target': { 'x': 'twrADC', 'y' : 'twrEmul3ADC'},
            'target_x' : 'twrEmul3ADC',
            'target_y' : 'twrADC',
            'name': 'twod', 
            'region': ['sevzero_all', 'sevthree_all', 'sevfour_all'],
            'axes' : {
                # 'xaxis': {'label': 'twrEmul3ADC', 'n_or_arr': 256, 'lo': 0, 'hi': 256},
                # 'yaxis': {'label': 'twrADC', 'n_or_arr': 256, 'lo': 0, 'hi': 256}
                'xaxis': {'label': 'twrEmul3ADC', 'n_or_arr': 256, 'lo': 0, 'hi': 256},
                'yaxis': {'label': 'twrADC', 'n_or_arr': 256, 'lo': 0, 'hi': 256}                
            }

        },        

        # ''

        # # 'twrEmul3ADC': {
        # #     'target': 'twrEmul3ADC',
        # #     'name': 'twrEmul3ADC', 
        # #     'region': ['sevzero_all', 'sevthree_all', 'sevfour_all', 'sevzero_TPratio_0_to_0p1'
        # # #                'sevzero_TPratio_0', 'sevthree_TPratio_0', 'sevfour_TPratio_0',
        # # #                'sevzero_TPratio_0_to_0p1', 'sevthree_TPratio_0_to_0p1', 'sevfour_TPratio_0_to_0p1',
        # # #                'sevzero_TPratio_0p1_to_0p2', 'sevthree_TPratio_0p1_to_0p2', 'sevfour_TPratio_0p1_to_0p2',
        # # #                'sevzero_TPratio_0p2_to_0p3', 'sevthree_TPratio_0p2_to_0p3', 'sevfour_TPratio_0p2_to_0p3',
        # # #                'sevzero_TPratio_0p3_to_0p4', 'sevthree_TPratio_0p3_to_0p4', 'sevfour_TPratio_0p3_to_0p4',
        # # #                'sevzero_TPratio_0p4_to_0p5', 'sevthree_TPratio_0p4_to_0p5', 'sevfour_TPratio_0p4_to_0p5',
        # # #                'sevzero_TPratio_0p5_to_0p6', 'sevthree_TPratio_0p5_to_0p6', 'sevfour_TPratio_0p5_to_0p6',
        # # #                'sevzero_TPratio_0p6_to_0p7', 'sevthree_TPratio_0p6_to_0p7', 'sevfour_TPratio_0p6_to_0p7',
        # # #                'sevzero_TPratio_0p7_to_0p8', 'sevthree_TPratio_0p7_to_0p8', 'sevfour_TPratio_0p7_to_0p8',
        # # #                'sevzero_TPratio_0p8_to_0p9', 'sevthree_TPratio_0p8_to_0p9', 'sevfour_TPratio_0p8_to_0p9',
        # # #                'sevzero_TPratio_0p9_to_1p0', 'sevthree_TPratio_0p9_to_1p0', 'sevfour_TPratio_0p9_to_1p0',
        # # #                'sevzero_TPratio_1',          'sevthree_TPratio_1',          'sevfour_TPratio_1'
            
        # #     ],
        # #     'axis': {'label': 'twrEmul3ADC', 'n_or_arr': 256, 'lo': 0, 'hi': 256}
        # # },  

        # 'twrADC': {
        #     'target': 'twrADC',
        #     'name': 'twrADC', 
        #     'region': ['sevzero_all', 'sevthree_all', 'sevfour_all',
        # #                'sevzero_TPratio_0', 'sevthree_TPratio_0', 'sevfour_TPratio_0',
        # #                'sevzero_TPratio_0_to_0p1', 'sevthree_TPratio_0_to_0p1', 'sevfour_TPratio_0_to_0p1',
        # #                'sevzero_TPratio_0p1_to_0p2', 'sevthree_TPratio_0p1_to_0p2', 'sevfour_TPratio_0p1_to_0p2',
        # #                'sevzero_TPratio_0p2_to_0p3', 'sevthree_TPratio_0p2_to_0p3', 'sevfour_TPratio_0p2_to_0p3',
        # #                'sevzero_TPratio_0p3_to_0p4', 'sevthree_TPratio_0p3_to_0p4', 'sevfour_TPratio_0p3_to_0p4',
        # #                'sevzero_TPratio_0p4_to_0p5', 'sevthree_TPratio_0p4_to_0p5', 'sevfour_TPratio_0p4_to_0p5',
        # #                'sevzero_TPratio_0p5_to_0p6', 'sevthree_TPratio_0p5_to_0p6', 'sevfour_TPratio_0p5_to_0p6',
        # #                'sevzero_TPratio_0p6_to_0p7', 'sevthree_TPratio_0p6_to_0p7', 'sevfour_TPratio_0p6_to_0p7',
        # #                'sevzero_TPratio_0p7_to_0p8', 'sevthree_TPratio_0p7_to_0p8', 'sevfour_TPratio_0p7_to_0p8',
        # #                'sevzero_TPratio_0p8_to_0p9', 'sevthree_TPratio_0p8_to_0p9', 'sevfour_TPratio_0p8_to_0p9',
        # #                'sevzero_TPratio_0p9_to_1p0', 'sevthree_TPratio_0p9_to_1p0', 'sevfour_TPratio_0p9_to_1p0',
        # #                'sevzero_TPratio_1',          'sevthree_TPratio_1',          'sevfour_TPratio_1'
            
        #     ],
        #     'axis': {'label': 'twrADC', 'n_or_arr': 256, 'lo': 0, 'hi': 256}
        # }, 

        # 'diagPlot': {
        #     'target': 'twrEmul3ADC:twrADC',
        #     'name': 'ratio', 
        #     'region': ['sevzero_all', 'sevthree_all', 'sevfour_all',
        # #                'sevzero_TPratio_0', 'sevthree_TPratio_0', 'sevfour_TPratio_0',
        # #                'sevzero_TPratio_0_to_0p1', 'sevthree_TPratio_0_to_0p1', 'sevfour_TPratio_0_to_0p1',
        # #                'sevzero_TPratio_0p1_to_0p2', 'sevthree_TPratio_0p1_to_0p2', 'sevfour_TPratio_0p1_to_0p2',
        # #                'sevzero_TPratio_0p2_to_0p3', 'sevthree_TPratio_0p2_to_0p3', 'sevfour_TPratio_0p2_to_0p3',
        # #                'sevzero_TPratio_0p3_to_0p4', 'sevthree_TPratio_0p3_to_0p4', 'sevfour_TPratio_0p3_to_0p4',
        # #                'sevzero_TPratio_0p4_to_0p5', 'sevthree_TPratio_0p4_to_0p5', 'sevfour_TPratio_0p4_to_0p5',
        # #                'sevzero_TPratio_0p5_to_0p6', 'sevthree_TPratio_0p5_to_0p6', 'sevfour_TPratio_0p5_to_0p6',
        # #                'sevzero_TPratio_0p6_to_0p7', 'sevthree_TPratio_0p6_to_0p7', 'sevfour_TPratio_0p6_to_0p7',
        # #                'sevzero_TPratio_0p7_to_0p8', 'sevthree_TPratio_0p7_to_0p8', 'sevfour_TPratio_0p7_to_0p8',
        # #                'sevzero_TPratio_0p8_to_0p9', 'sevthree_TPratio_0p8_to_0p9', 'sevfour_TPratio_0p8_to_0p9',
        # #                'sevzero_TPratio_0p9_to_1p0', 'sevthree_TPratio_0p9_to_1p0', 'sevfour_TPratio_0p9_to_1p0',
        # #                'sevzero_TPratio_1',          'sevthree_TPratio_1',          'sevfour_TPratio_1'
            
        #     ],
        #     'axis': {'label': 'twrADC', 'n_or_arr': 256, 'lo': 0, 'hi': 256}
        # },                                         

    # }

    # twoD_histograms = {
    #     'realVsEmu': {
    #           'xvar' : 'twrADC',
    #           'yvar' : 'twrEmul3ADC',
    # #         'target': 'twrEmul3ADC:twrADC',
    #           'name': 'realVsEmu', 
    #         'region': ['sevzero'],
    #         'axis': {'label': 'realVsEmu', 'n_or_arr': 256, 'lo': 0, 'hi': 256}
    #     },    

    }

    selection = {

        ##-- Sev 0
            "sevzero_all" : [
                "event.time != -999",
                "event.sevlv == 0",
                # "event.twrADC > 0"
	    ],  

        #     "sevzero_TPratio_0" : [
        #         "event.time != -999",
        #         "event.sevlv == 0", 
        #         "( (event.twrEmul3ADC / (event.twrADC + 0.000001)) == 0)"
	    # ],  

        #     "sevzero_TPratio_0_to_0p1" : [
        #         "event.time != -999",
        #         "event.sevlv == 0", 
        #         # "( np.divide(event.twrEmul3ADC, event.twrADC, np.zeros_like(event.twrEmul3ADC), where=event.twrADC!=0 ) < 0.1)"
        #         # ((event.twrEmul3ADC / (event.twrADC)) > 0)", 
        #         # "((event.twrEmul3ADC / (event.twrADC )) < 0.1)"

        #         # "( ((event.twrEmul3ADC / (event.twrADC)) > 0)", "((event.twrEmul3ADC / (event.twrADC )) < 0.1)"
	    # ],   

        #     "sevzero_TPratio_0p1_to_0p2" : [
        #         "event.time != -999",
        #         "event.sevlv == 0", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.1)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.2) )"
	    # ],    

        #     "sevzero_TPratio_0p2_to_0p3" : [
        #         "event.time != -999",
        #         "event.sevlv == 0", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.2)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.3) )"
	    # ],    

        #     "sevzero_TPratio_0p3_to_0p4" : [
        #         "event.time != -999",
        #         "event.sevlv == 0", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.3)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.4) )"
	    # ],    

        #     "sevzero_TPratio_0p4_to_0p5" : [
        #         "event.time != -999",
        #         "event.sevlv == 0", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.4)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.5) )"
	    # ],    

        #     "sevzero_TPratio_0p5_to_0p6" : [
        #         "event.time != -999",
        #         "event.sevlv == 0", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.5)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.6) )"
	    # ],    

        #     "sevzero_TPratio_0p6_to_0p7" : [
        #         "event.time != -999",
        #         "event.sevlv == 0", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.6)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.7) )"
	    # ],  

        #     "sevzero_TPratio_0p7_to_0p8" : [
        #         "event.time != -999",
        #         "event.sevlv == 0", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.7)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.8) )"
	    # ],  

        #     "sevzero_TPratio_0p8_to_0p9" : [
        #         "event.time != -999",
        #         "event.sevlv == 0", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.8)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.9) )"
	    # ],  

        #     "sevzero_TPratio_0p9_to_1p0" : [
        #         "event.time != -999",
        #         "event.sevlv == 0", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.9)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 1.0) )"
	    # ],   

        #     "sevzero_TPratio_1" : [
        #         "event.time != -999",
        #         "event.sevlv == 0", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) == 1.0) )"
	    # ],            

        ##-- Sev 3 
            "sevthree_all" : [
                "event.time != -999",
                "event.sevlv == 3",
                # "event.twrADC > 0"
	    ],  

        #     "sevthree_TPratio_0" : [
        #         "event.time != -999",
        #         "event.sevlv == 3", 
        #         "( (event.twrEmul3ADC / (event.twrADC + 0.000001)) == 0)"
	    # ],  

        #     "sevthree_TPratio_0_to_0p1" : [
        #         "event.time != -999",
        #         "event.sevlv == 3", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) > 0)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.1)"
	    # ],  

        #     "sevthree_TPratio_0p1_to_0p2" : [
        #         "event.time != -999",
        #         "event.sevlv == 3", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.1)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.2) )"
	    # ],    

        #     "sevthree_TPratio_0p2_to_0p3" : [
        #         "event.time != -999",
        #         "event.sevlv == 3", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.2)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.3) )"
	    # ],    

        #     "sevthree_TPratio_0p3_to_0p4" : [
        #         "event.time != -999",
        #         "event.sevlv == 3", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.3)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.4) )"
	    # ],    

        #     "sevthree_TPratio_0p4_to_0p5" : [
        #         "event.time != -999",
        #         "event.sevlv == 3", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.4)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.5) )"
	    # ],    

        #     "sevthree_TPratio_0p5_to_0p6" : [
        #         "event.time != -999",
        #         "event.sevlv == 3", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.5)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.6) )"
	    # ],    

        #     "sevthree_TPratio_0p6_to_0p7" : [
        #         "event.time != -999",
        #         "event.sevlv == 3", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.6)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.7) )"
	    # ],  

        #     "sevthree_TPratio_0p7_to_0p8" : [
        #         "event.time != -999",
        #         "event.sevlv == 3", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.7)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.8) )"
	    # ],  

        #     "sevthree_TPratio_0p8_to_0p9" : [
        #         "event.time != -999",
        #         "event.sevlv == 3", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.8)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.9) )"
	    # ],  

        #     "sevthree_TPratio_0p9_to_1p0" : [
        #         "event.time != -999",
        #         "event.sevlv == 3", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.9)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 1.0) )"
	    # ],   

        #     "sevthree_TPratio_1" : [
        #         "event.time != -999",
        #         "event.sevlv == 3", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) == 1.0) )"
	    # ],        

        ##-- Sev 4 
            "sevfour_all" : [
                "event.time != -999",
                "event.sevlv == 4",
                # "event.twrADC > 0"
	    ],  

        #     "sevfour_TPratio_0" : [
        #         "event.time != -999",
        #         "event.sevlv == 4", 
        #         "( (event.twrEmul3ADC / (event.twrADC + 0.000001)) == 0)"
	    # ],  

        #     "sevfour_TPratio_0_to_0p1" : [
        #         "event.time != -999",
        #         "event.sevlv == 4", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) > 0)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.1)"
	    # ],  

        #     "sevfour_TPratio_0p1_to_0p2" : [
        #         "event.time != -999",
        #         "event.sevlv == 4", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.1)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.2) )"
	    # ],    

        #     "sevfour_TPratio_0p2_to_0p3" : [
        #         "event.time != -999",
        #         "event.sevlv == 4", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.2)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.3) )"
	    # ],    

        #     "sevfour_TPratio_0p3_to_0p4" : [
        #         "event.time != -999",
        #         "event.sevlv == 4", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.3)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.4) )"
	    # ],    

        #     "sevfour_TPratio_0p4_to_0p5" : [
        #         "event.time != -999",
        #         "event.sevlv == 4", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.4)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.5) )"
	    # ],    

        #     "sevfour_TPratio_0p5_to_0p6" : [
        #         "event.time != -999",
        #         "event.sevlv == 4", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.5)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.6) )"
	    # ],    

        #     "sevfour_TPratio_0p6_to_0p7" : [
        #         "event.time != -999",
        #         "event.sevlv == 4", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.6)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.7) )"
	    # ],  

        #     "sevfour_TPratio_0p7_to_0p8" : [
        #         "event.time != -999",
        #         "event.sevlv == 4", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.7)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.8) )"
	    # ],  

        #     "sevfour_TPratio_0p8_to_0p9" : [
        #         "event.time != -999",
        #         "event.sevlv == 4", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.8)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 0.9) )"
	    # ],  

        #     "sevfour_TPratio_0p9_to_1p0" : [
        #         "event.time != -999",
        #         "event.sevlv == 4", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) >= 0.9)", "((event.twrEmul3ADC / (event.twrADC + 0.000001)) < 1.0) )"
	    # ],   

        #     "sevfour_TPratio_1" : [
        #         "event.time != -999",
        #         "event.sevlv == 4", 
        #         "( ((event.twrEmul3ADC / (event.twrADC + 0.000001)) == 1.0) )"
	    # ],  


        #     "sevzerotagged" : [
        #         "event.time != -999",
        #         "event.sevlv == 0",
        #         "(event.twrEmul3ADC < event.twrADC)" ##-- Emulated TP ADC is less. This means at least some was zeroed
	    # ],   

        #     "sevthreetagged" : [
        #         "event.time != -999",
        #         "event.sevlv == 3",
        #         "(event.twrEmul3ADC < event.twrADC)"
	    # ],   

        #     "sevfourtagged" : [
        #         "event.time != -999",
        #         "event.sevlv == 4",
        #         "(event.twrEmul3ADC < event.twrADC)"
	    # ],                                

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
        # return f'{name}_{self.sample}_{region}{self.syst_suffix}'
