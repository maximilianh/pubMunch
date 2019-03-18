#!/usr/bin/env python
# Author: Greg Caporaso (gregcaporaso@gmail.com)
# mutation_finder.py

""" Description
File created on 25 Jan 2007.
This is the Python implementation of MutationFinder. It can be used in one of two ways:
  1) as a script for extraction mutation mentions from text sources
  2) as library code imported to other Python applications

Revision history:
7/2/2007 Greg Caporaso: Added flag '-n' to output normalized mutations instead
 of extracted mentions.
8/3/2007 Greg Caporaso: Added handling of ambiguous amino acid abbreviations, 
B (ASX), G (GLX), and X (XAA). These will also be added to the regular 
expressions in response to a feature request.

Copyright (c) 2007 Regents of the University of Colorado
Please refer to licensing agreement at MUTATIONFINDER_HOME/doc/license.txt
"""
from __future__ import print_function
# Define the extension that should be used when creating mutation 
# finder output files -- the default is 'mf'
mutation_finder_output_file_extension = 'mf'

# Set the install directory
mutation_finder_home = './'

## Users should make no modifications beyond this line (i.e., the developers will
## not provide support for any issues you encounter as a result of modificatations
## to the code). If you do make modifications, be sure to run the unit tests
## (test_mutationfinder.py) to ensure that all tests still pass.

version_number = '1.0'

# --MAX--
# re2 is A LOT faster (>10x when I tried, 15 minutes versus 233 minutes on cluster)
try:
    from re2 import compile, VERBOSE, IGNORECASE
except:
    print("Failed to import module re2, falling back to re module (a lot slower)")
    from re import compile, VERBOSE, IGNORECASE

from os.path import exists
from sys import exit
from os.path import *
   
# A dictionary mapping three-letter amino acids codes onto one-letter
# amino acid codes
amino_acid_three_to_one_letter_map = \
    dict([('ALA','A'),('GLY','G'),('LEU','L'),('MET','M'),\
     ('PHE','F'),('TRP','W'),('LYS','K'),('GLN','Q'),('GLU','E'),('SER','S'),\
     ('PRO','P'),('VAL','V'),('ILE','I'),('CYS','C'),('TYR','Y'),('HIS','H'),\
     ('ARG','R'),('ASN','N'),('ASP','D'),('THR','T'),('XAA','X'),('GLX','Z'),\
     ('ASX','B')])

# A dictionary mapping amino acid names to their one-letter abbreviations
amino_acid_name_to_one_letter_map = \
    dict([('ALANINE','A'),('GLYCINE','G'),('LEUCINE','L'),\
     ('METHIONINE','M'),('PHENYLALANINE','F'),('TRYPTOPHAN','W'),\
     ('LYSINE','K'),('GLUTAMINE','Q'),('GLUTAMIC ACID','E'),\
     ('GLUTAMATE','E'),('ASPARTATE','D'),('SERINE','S'),\
     ('PROLINE','P'),('VALINE','V'),('ISOLEUCINE','I'),('CYSTEINE','C'),\
     ('TYROSINE','Y'),('HISTIDINE','H'),('ARGININE','R'),\
     ('ASPARAGINE','N'),('ASPARTIC ACID','D'),('THREONINE','T')])

# Set the name of the default file containing the MutationFinder 
# regular expressions
default_regular_expression_filepath = \
    ''.join([mutation_finder_home,'regex.txt'])

################
# The Mutation and PointMutation objects 
# 
# Mutation is a base class for different types of mutations. Currently the
# only Mutation type defined is a PointMutation, but in the future Insertion
# and Deletion objects may also be defined. 
# 
# Each mutation mention returned by the extraction systems is packaged into a 
# PointMutation object. 
#
# A wrapper function is also provided for creating PointMutation objects from
# wNm-formated mutation mentions. This is purely for convenience.
#
################

class MutationError(Exception):
        pass

class Mutation(object):
    """ A base class for storing information about mutations """

    def __init__(self, Position):
        """ Initalize the object 
            
            Position: the sequence position or start position of the mutation
                (must be castable to an int)
        """
        try:
            self.__position = int(Position)
        except ValueError:
            raise MutationError("Position must be an integer")
        if self.__position < 1:
            raise MutationError("Position must be greater than 0")
    
    def _get_position(self):
        return self.__position
    Position = property(_get_position)

    def __str__(self):
        raise NotImplementedError('Mutation subclasses must override str()')

    def __eq__(self,other):
        raise NotImplementedError('Mutation subclasses must override ==')

    def __ne__(self,other):
        raise NotImplementedError('Mutation subclasses must override !-')

    def __hash__(self):
        raise NotImplementedError('Mutation subclasses must override hash()')

class PointMutation(Mutation):
    """ A class for storing information about protein point mutations

    """

    # Define a mapping for residue identity inputs to one-letter
    # abbreviations. For simplicty of the normalization procedure, a 
    # one-letter to one-letter 'mapping' is also included. This 
    # eliminates the need for an independent validation step, since
    # any valid identity which is passed in will be a key in this dict, 
    # and it avoids having to analyze which format the input residue 
    # was passed in as.  
    _abbreviation_lookup = dict(zip(list('ABCDEFGHIKLMNPQRSTVWXYZ'),\
                                    list('ABCDEFGHIKLMNPQRSTVWXYZ')))
    _abbreviation_lookup.update(amino_acid_three_to_one_letter_map)
    _abbreviation_lookup.update(amino_acid_name_to_one_letter_map)

    def __init__(self,Position,WtResidue,MutResidue,regexNum):
        """ Initalize the object and call the base class init 

            Position: the sequence position or start position of the mutation
                (castable to an int)
            WtResidue: the wild-type (pre-mutation) residue identity (a string)
            MutReside: the mutant (post-mutation) residue identity (a string)

            Residues identities are validated to ensure that they are within 
             the canonical set of amino acid residues are normalized to their
             one-letter abbreviations.
        """
        self.__wt_residue = self._normalize_residue_identity(WtResidue)
        self.__mut_residue = self._normalize_residue_identity(MutResidue)
        self.regexNum = regexNum
        Mutation.__init__(self,Position=Position)

    def _normalize_residue_identity(self,residue):
        """ Normalize three-letter and full residue names to their 
             one-letter abbreviations. If a residue identity is passed in
             which does not fall into the set of canonical amino acids
             a MutationError is raised.

        """
        try:
                # convert residue to its single letter abbreviation after
                # converting it to uppercase (so lookup is case-insensitive)
                return self._abbreviation_lookup[residue.upper()]
        except AttributeError:
                # if residue cannot be converted to uppercase, it is not a 
                # string, so raise an error
                raise MutationError('Residue must be a string')
        except KeyError:
                # if residue is not a key in self._abbreviation_lookup, it
                # it is not a standard amino acid residue, so raise an error
                raise MutationError('Input residue not recognized, must be a standard residue: '\
                  + residue)

    def _get_wt_residue(self):
        return self.__wt_residue
    WtResidue = property(_get_wt_residue)

    def _get_mut_residue(self):
        return self.__mut_residue
    MutResidue = property(_get_mut_residue)

    # Position property defined in Mutation

    def __str__(self):
        """ Override str(), returns mutation as a string in wNm format"""
        return ''.join([self.__wt_residue,str(self.Position),\
            self.__mut_residue])

    def __eq__(self,other):
        """ Override ==

            Two PointMutation objects are equal if their Position, WtResidue,
             and MutResidue values are all equal.
        """
        return self.Position == other.Position and \
               self.__wt_residue == other.__wt_residue and \
               self.__mut_residue == other.__mut_residue 

    def __ne__(self,other):
        """ Override !=

            Two PointMutation obects are not equal if either their Position,
             WtResidue, or MutResidue values differ.
        """
        return not self == other

    def __hash__(self):
        """ Override hash() """
        return hash(str(type(self)) + str(self))

def PointMutation_from_wNm(wNm):
    """ Create PointMutation from wNm-format, single-letter abbreviated mention
        
        This wrapper function creates a PointMutation object from a 
            mutation mention formatted in wNm-format, where w and m are the
            wild-type and mutant amino acids in their SINGLE-LETTER 
            abbreviations, and N is an integer representing the sequence
            position.     
    """

    try:
        return PointMutation(int(wNm[1:-1]),wNm[0],wNm[-1])
    except ValueError:
        raise MutationError('Improperly formatted mutation mention:  ' + wNm)
#######

class MutationExtractor(object):
    """ A base class for extracting Mutations from text """

    def __init__(self,ignorecase=True):
        """ Initialize the object """
        pass

class BaselineMutationExtractor(MutationExtractor):
    """ A class for extracting point mutations mentions from text 

        This class is based on the MuteXt system, described in 
         Horn et al., (2004). Their rules for matching point mutations
         are implemented, but their sequence-based validation step is
         not. This class is the 'baseline system' discussed in 
         Caporaso et al., (2007), and can be used to reproduce those 
         results. Exact instructions for reproducing those results are
         provided in the example code <??WHERE WILL EXAMPLE CODE BE??>

    """


    # MuteXt matches amino acid single letter abbreviations in uppercase
    single_letter_match = r''.join(amino_acid_three_to_one_letter_map.values())
    # MuteXt only matches amino acid three letter abbreviations in titlecase
    # (i.e. first letter in uppercase, all others in lowercase)
    triple_letter_match = r'|'.join(\
        [aa.title() for aa in amino_acid_three_to_one_letter_map.keys()])
    # The MuteXt paper doesn't speicfy what cases are used for matching full 
    # residue mentions. We allow lowercase or titlecase for maximum recall --
    # precision seems unlikely to be affected by this.
    full_name_match = r'|'.join(\
        [aa.lower() for aa in amino_acid_name_to_one_letter_map.keys()] +\
        [aa.title() for aa in amino_acid_name_to_one_letter_map.keys()])

    single_wt_res_match = \
           r''.join([r'(?P<wt_res>[',single_letter_match,r'])'])
    single_mut_res_match = \
           r''.join([r'(?P<mut_res>[',single_letter_match,r'])'])

    triple_wt_res_match = \
           r''.join([r'(?P<wt_res>(',triple_letter_match,r'))'])
    triple_mut_res_match = \
           r''.join([r'(?P<mut_res>(',triple_letter_match,r'))'])

    full_mut_res_match = \
           r''.join([r'(?P<mut_res>(',full_name_match,r'))'])

    position_match = r"""(?P<pos>[1-9][0-9]*)"""
    
    def __init__(self):
        """ Initialize the object """
        MutationExtractor.__init__(self)
        word_regex_patterns = self._build_word_regex_patterns()
        string_regex_patterns = self._build_string_regex_patterns()

        self._word_regexs = []
        self._string_regexs = []
        self._replace_regex = compile('[^a-zA-Z0-9\s]')

        # Compile the regular expressions
        for regex_pattern in word_regex_patterns:
            self._word_regexs.append(compile(regex_pattern))
        for regex_pattern in string_regex_patterns:
            self._string_regexs.append(compile(regex_pattern))

    def _build_string_regex_patterns(self):
        """ build the sentence-level regex patterns

            These patterns match an xN followed by a mutant residue
            mention within ten words
            (e.g. 'we mutated Ser42 to glycine')
            The wt residue can be a one- or three-letter abbreviation, and the
            mt residue can be a three-letter abbreviation or full name.
        """
        return [
            r''.join([r'(^|\s)',self.single_wt_res_match,\
                      self.position_match,r'\s(\w+\s){,9}',\
                      self.triple_mut_res_match,r'(\s|$)']),\
            r''.join([r'(^|\s)',self.single_wt_res_match,\
                      self.position_match,r'\s(\w+\s){,9}',\
                      self.full_mut_res_match,r'(\s|$)']),\
            r''.join([r'(^|\s)',self.triple_wt_res_match,\
                      self.position_match,r'\s(\w+\s){,9}',\
                      self.triple_mut_res_match,r'(\s|$)']),\
            r''.join([r'(^|\s)',self.triple_wt_res_match,\
                      self.position_match,r'\s(\w+\s){,9}',\
                      self.full_mut_res_match,r'(\s|$)'])
            ]


    def _build_word_regex_patterns(self):
        """ Build the word-level reqex patterns

            These patterns match wNm format mutations using either 
                one-letter abbreviations OR three-letter abbreviations, but
                not a mixture. 
                (e.g. A42G and Ala42Gly will match, but not A42Gly)

        """
        return [
            r''.join(['^',self.single_wt_res_match,\
                      self.position_match,\
                      self.single_mut_res_match,'$']),\
            r''.join(['^',self.triple_wt_res_match,\
                      self.position_match,\
                      self.triple_mut_res_match,'$']),\
            ]

    def __call__(self,raw_text):
        """ Extract point mutations mentions from raw_text and return them in a dict
             
             raw_text: the text from which mutations should be extracted

             IT IS NOT POSSIBLE TO STORE SPANS WHEN EXTRACTING MUTATIONS WITH
              BaselineMutationExtractor. Because MuteXt splits on sentences and
              words, and removes alphanumeric characters from within words, the
              mappings to original character-offsets get complicated. 
             MutationFinder does, however, return spans.

        """
        result = {}

        # Apply patterns which work on the word level and attempt to
        # find xNy matches
        for regex in self._word_regexs:
            for word in self._preprocess_words(raw_text):
                for m in regex.finditer(word):
                    current_mutation = \
                      PointMutation(m.group('pos'),m.group('wt_res'),\
                                m.group('mut_res'))

                    try:
                        result[current_mutation] += 1
                    except KeyError:
                        result[current_mutation] = 1

        # Apply patterns which work on the sentence level and attempt
        # to find a mutant residue up to ten words ahead of a xN match
        for regex in self._string_regexs:
            for sentence in self._preprocess_sentences(raw_text):
                for m in regex.finditer(sentence):
                    current_mutation = \
                      PointMutation(m.group('pos'),m.group('wt_res'),\
                                m.group('mut_res'))

                    try:
                        result[current_mutation] += 1
                    except KeyError:
                        result[current_mutation] = 1
        return result


    def _preprocess_sentences(self,raw_text):
        """ Preprocess input text as MuteXt does

            When working on sentences, MuteXt splits on sentence
            breaks and removes all non-alphanumeric characters.
        """
        return [self._replace_regex.sub('',sentence).strip()\
                for sentence in raw_text.split('.')]


    def _preprocess_words(self,raw_text):
        """ Preprocess input text as MuteXt does

            When working on words, MuteXt splits an input string
            on 'words' and removes all non-alphanumeric characters.
            It is not clear how the MuteXt system splits words. If they
            split on non-alphanumeric (i.e., '\b'), they would miss many 
            of the patterns they report to identify (e.g. G-142-A). We 
            therefore simplisticly split on whitespace here.
        """
        return [self._replace_regex.sub('',word)\
                for word in raw_text.split()]

class MutationFinder(MutationExtractor):

    def __init__(self,regular_expressions):
        """ Initialize the object 

            regular_expressions: an interative set of regular expressions to
                be applied for extracting mutations. These are in the 
                default python syntax (i.e., perl regular expressions), with 
                the single exception being that regular expressions which
                should be performed in a case sensitive manner should be 
                followed by the string '[CASE_SENSITIVE]', with no spaces 
                between it and the regular expression. 
                This can be a list, a file, or any other object which supports
                iteration. For an example, you should refer to the regex.txt
                file in the MutationFinder directory.

        """
        MutationExtractor.__init__(self)
        self._regular_expressions = []

        for regular_expression in regular_expressions:
            if regular_expression.endswith('[CASE_SENSITIVE]'):
                self._regular_expressions.append(\
                 compile(regular_expression[:regular_expression.rindex('[')]))
            else:
                self._regular_expressions.append(\
                 compile(regular_expression,IGNORECASE))

    def _post_process(self,mutations):
        """ Perform precision increasing post-processing steps

            Remove false positives indicated by:
              -> mutant and wild-type residues being identical (e.g. A42A)

        """

        for mutation in mutations.keys():
            if mutation.WtResidue == mutation.MutResidue:
                del mutations[mutation]

    def __call__(self,raw_text):
        """ Extract point mutations mentions from raw_text and return them in a dict
             
             raw_text: a string of text

            The result of this method is a dict mapping PointMutation objects to
             a list of spans where they were identified. Spans are presented in the     
             form of character-offsets in text. If counts for each mention are 
             required instead of spans, apply len() to each value to convert the 
             list of spans to a count. 

            Example result:
             raw_text: 'We constructed A42G and L22G, and crystalized A42G.'
             result = {PointMutation(42,'A','G'):[(15,19),(46,50)], 
                       PointMutation(22,'L','G'):[(24,28)]}

             Note that the spans won't necessarily be in increasing order, due 
              to the order of processing regular expressions.


        """
        result = {}
        # --MAX--
        # added the regexNum to get a better handle on what is important of the regexes
        for regexNum, regular_expression in enumerate(self._regular_expressions):
            for m in regular_expression.finditer(raw_text):
                current_mutation = \
                  PointMutation(m.group('pos'),m.group('wt_res'),\
                            m.group('mut_res'), regexNum)
                # The span of the mutation is calcluated as the min
                # start span of the three components and the max end span
                # of the three components -- these are then packed up as 
                # a tuple.
                span = min(m.span('wt_res')[0],\
                           m.span('pos')[0],\
                           m.span('mut_res')[0]),\
                       max(m.span('wt_res')[1],\
                           m.span('pos')[1],\
                           m.span('mut_res')[1])
                try:
                    result[current_mutation].append(span) 
                except KeyError:
                    result[current_mutation] = [span]

        self._post_process(result)
        return result

#####
# Functions provided for script functionality
#
#####

class MutationFinderError(RuntimeError):
    pass

def parse_command_line_parameters():
    """ Parses command line arguments """
    usage = "usage: %prog [options] input_file1.tsv input_file2.tsv ..."
    version = ' '.join(['Version: %prog',version_number])
    parser = OptionParser(usage=usage,version=version)
    
    # A binary 'verbose' flag
    parser.add_option('-v','--verbose',action='store_true',dest='verbose',\
                help="print information on progress -- useful for debugging" +\
                     " [default: %default]")

    # An example string option
    #result.add_option('-i','--input_dir',action='store',\
    #                  type='string',dest='input_dir')
    parser.add_option('-o','--output_dir',action='store',\
                type='string',dest='output_dir',\
                help="Specify the directory where output files "+ \
                "should be stored [default: %default]")
    parser.add_option('-b','--use_baseline_system',action='store_true',\
                dest='use_baseline_system',\
                help="Use the baseline system (from Caporaso et al., 2007) " +\
                    "instead of MutationFinder for extracting mutations" +\
                    " [default: %default]")
    parser.add_option('-s','--store_spans',action='store_true',\
                dest='store_spans',\
                help='Record the span, in byte offsets, where the mutation ' +\
                     'was identified. [default: %default]')
    parser.add_option('-n','--output_normalized_mutations',action='store_true',\
                dest='output_normalized_mutations',\
                help='Return normalized mutations rather than extracted ' +\
                     'mentions [default: %default]')
    parser.add_option('-r','--regular_expression_filepath',action='store',\
                type='string',dest='regular_expression_filepath',\
                help="The regular expressions file to be used when "+ \
                " constructing the MutationFinder [default: %default]")

    # Set default values here if they should be other than None
    parser.set_defaults(verbose=False,output_dir='./',\
        use_baseline_system=False,store_spans=False,
        regular_expression_filepath=default_regular_expression_filepath)
    
    opts, args = parser.parse_args()

    # verify that at least one input file was provided to work on 
    if len(args) < 1:
        parser.error("No input files specified -- there's nothing to do.")
    if opts.use_baseline_system and opts.store_spans:
        parser.error(\
            "The baseline system cannot return spans: -s and -b cannot both be specified.")   
    if opts.output_normalized_mutations and opts.store_spans:
        parser.error(\
            "-n and -s options are incompatible. Please specify one or the other.")
 
    return opts, args

def mutation_finder_from_regex_filepath(\
    regular_expression_filepath=default_regular_expression_filepath):
    """ Constructs a MutationFinder object using regular expressions in a file

        By default, the regular expressions used are derived from 
            default_regular_expression_filepath. This is defined at the top of
            this file, or can be passed as an option to the script version of 
            this code.

    """
    try:
        regular_expressions_file = open(regular_expression_filepath)
    except IOError:
        print('Can not open the regular expression file:', \
            regular_expression_filepath)
        print('If using the default regular expression file and you are running',\
            'mutation finder from a directory other than where it is insalled,',\
            'be sure to set the mutation_finder_home variable in mutation_finder.py')
        exit(-1)
    
    regular_expressions = []
    # Read in and store regular expression, ignoring lines that begin with '#'
    for line in regular_expressions_file:
        line = line.strip()
        if not line.startswith('#'):
            regular_expressions.append(line)
    
    return MutationFinder(regular_expressions)

def extract_mutations_from_string(text,mutation_extractor):
    """ Applies mutation_extractor to text and return the mutations in a dict

        text: a single string containing the text which mutations should be
            extracted from
        mutation_extractor: the MutationExtractor object to be used for the
            extraction process

    """
    return mutation_extractor(text)

def extract_mutations_from_lines(lines,mutation_extractor):
    """ An iterator for extracting mutations from lines

        lines: an iterable object where each item is a tab-delimited
            string where the first field is a unique identifier and the
            remaining fields comprise a single text source; most commonly
            this will be a file or list
        mutation_extractor: the MutationExtractor object to be used for the
            extraction process
    """
    for line in lines:
        fields = line.strip().split('\t')
        try:
            # the data before the first tab is the text source identifier
            #identifier = fields[0]
            # the data after the first tab (including any subsequent tabs)
            # is the text
            text = '\t'.join(fields[1:])
            yield extract_mutations_from_string(text,mutation_extractor)
        except IndexError:
            # Ignore blank lines
            pass

def extract_mutations_from_lines_to_dict(lines,\
        mutation_extractor,store_spans=False):
    """ 
        lines: an iterable object where each item is a tab-delimited
            string where the first field is a unique identifier and the
            remaining fields comprise a single text source; most commonly
            this will be a file or list
        mutation_extractor: the MutationExtractor object to be used for the
            extraction process
    """
    result = dict(list(extract_mutations_from_lines(lines,mutation_extractor)))
    if store_spans:
        return result
    # If spans are not requested, replace the span lists with counts
    else:
        for id, mutations in result.items():
            for mutation,value in mutations.items():
                try:
                    # Value is a list of spans
                    result[id][mutation] = len(value)
                except TypeError:
                    # Value is a count
                    result[id][mutation] = value
        return result 

def extract_mutations_from_lines_to_file(lines,output_filepath,\
   mutation_extractor,store_spans=False,output_normalized_mutations=False):
    """ 
        This function extracts mutations from an iterable object
            containing the lines of a file (i.e. either a list or file object) 
            and writes the mutations to an output file. The text span (in 
            character offset) can also be recorded for each mutation mention, 
            if the MutationExtractor supports storing spans. Currently 
            BaselineMutationExtractor does not support spans, while 
            MutationFinder does.

        lines: an iterable object where each item is a tab-delimited
            string where the first field is a unique identifier and the
            remaining fields comprise a single text source;
            this will be a file or list.
        output_filepath: the path and filename where the output should be 
            written
        mutation_extractor: the MutationExtractor object to be used for the
            extraction process
        store_spans: a boolean defining whether spans for each mutation mention
            should be stored -- note the the MutationExtractor must support
            spans. (For example, the BaselineMutationExtractor cannot record
            spans while MutationFinder does. Specifying the former and 
            store_spans=True will result in a MutationFinderError.
            [default=False]
        output_normalized_mutations: a boolean defining wheter normalized
            mutations should be printed instead of extracted mentions. 
            [default=False] (Note: this is incompatible with store_spans=True)
        
    """
    # Determine if the output file already exists -- if so, print a message
    # and quit.
    assert not (store_spans and output_normalized_mutations),\
     "store_spans==True and output_normalized_mutations==True are incompatible"
    #if exists(output_filepath):
            #print "Output file already exists: " + output_filepath + \
                  #"\n Please either rename or move the existing file."
            #exit(1)

    # Open the output file for writing
    #try:
        #output_file = open(output_filepath,'w')
    #except IOError:
        #print 'Could not open the output file for writing: ', output_filepath
        #exit(1)

    # Extract mutations from each line, and iterate over them
    for identifier,mutations in \
            extract_mutations_from_lines(lines,mutation_extractor):
        # The identifier will always be stored -- create a list to begin
        # storing the information which will be written on the current line
        output_fields = [identifier]
        if output_normalized_mutations:
            # If we're only printing out the normalized mutations, this is 
            # easy. Generate a list of each mutation, and append them to 
            # output_fields.
            output_fields += map(str,dict(mutations).keys())
        else:
            # Iterate over the mutations and their values -- note that for 
            # MutationExtractors which support spans, value will be equal
            # to a list of tuples containing the span of each mention. For
            # MutationExtractors which don't support spans 
            # (e.g. BaselineMutationExtractor) value will be the count of the
            # mentions. 
            for mutation,value in mutations.items():
                if store_spans:
                    try:
                    # Write the current mutation and its spans
                        output_fields +=\
                         [''.join([str(mutation),':',str(span[0]),',',\
                                str(span[1])]) for span in value]
                    # Raise a MutationFinderError if an attempt is being made to 
                    # store spans from a MutationExtractor which does not support 
                    # spans
                    except TypeError:
                        raise MutationFinderError('Attempting to access spans from a MutationExtractor ' +\
                         'which cannot store them.')
                else:
                    # For MutationExtractors which support spans, determine the
                    # number of mentions by counting the spans. Write the 
                    # mutation the number of times it was mentioned.
                    try:
                        output_fields += [str(mutation)]*len(value)
                    # For MutationExtractors which don't support spans, the count
                    # is the value. Write the mutation the number of times it was
                    # mentioned.
                    except TypeError:
                        output_fields += [str(mutation)]*value
        # Create and write the tab-delimited line
        yield output_fields
        #output_file.write('\t'.join(output_fields))
        #output_file.write('\n')

    #output_file.close()

def filename_from_filepath(filepath):
    """ Strip the path off a filepath to get a filename"""
    try:
        return filepath[filepath.rindex('/')+1:]
    except ValueError:
        return filepath

def build_output_filepath(output_dir,input_filepath):
    """ Construct the filepath for the output file 

        output_dir: the path to where output should be written
        input_filepath: the path to the input file -- this is used to
            construct the output file name; the value of this parameter
            is checked explicitly to ensure that an empty string is not
            passed in, since that could end up creating a hidden file
            which could be confusing and annoying


    """
    if input_filepath:
        output_filename = \
            ''.join([filename_from_filepath(input_filepath),'.',\
                mutation_finder_output_file_extension])
    else:
        raise MutationFinderError('Must pass non-empty input filepath to construct output filename')

    if output_dir.endswith('/'):
        join_text = ''
    else: 
        join_text = '/'
    return join_text.join([output_dir,output_filename])

if __name__ == "__main__":
    from optparse import OptionParser

    opts,args = parse_command_line_parameters()
    
    # Construct the mutation extractor object -- note that users can specify 
    # to use either the BaselineMutationExtractor or MutationFinder with the 
    # -b option
    if opts.use_baseline_system:
        mutation_extractor = BaselineMutationExtractor()
    else:
        mutation_extractor = mutation_finder_from_regex_filepath(\
            opts.regular_expression_filepath)

    for input_filepath in args:
        # Create a handle for the input file, and process any errors
        try:
            input_file = open(input_filepath)
        except IOError:
            print('Can not open specified input file: ', input_filepath)
            exit(1)

        #output_filepath = build_output_filepath(opts.output_dir,input_filepath)
           
        for row in extract_mutations_from_lines_to_file(input_file,None,\
            mutation_extractor,opts.store_spans,opts.output_normalized_mutations):
            print(row)
    
    # All done, exit cleanly
    exit(0)

headers = ["start", "end", "pmid", "regexId", "wtRes", "pos", "mutRes"]
mutFinder = None
def startup(paramDict):
    global mutFinder
    regexFname = join(dirname(__file__), "data", "mutationFinder.regex2.txt")
    #regexFname = "regex2.txt"
    mutFinder = mutation_finder_from_regex_filepath(regexFname)

blackList = set(["E24377A"])

def annotateFile(article, file):
    text = file.content
    rows = []
    for mut, spans in mutFinder(text).iteritems():
        #print repr(mutations)
        for start, end in spans:
            #print str(mut)
            word = text[start:end]
            if word in blackList:
                continue
            row = [start, end, article.pmid, mut.regexNum, mut._get_wt_residue(), \
                str(mut.Position), mut._get_mut_residue()]
            yield row
        #print row

        #fields = map(str,dict(mutations).keys())
        #yield fields
