################################################
# file: Makefile
# Makefile for latex
#
# @Author: SUN, Tong
# Copyright (c)2001, Tong SUN, all right reserved
# @Version: $Date: 2001/09/04 06:21:20 $ $Revision: 1.2.1.4 $
# @Home URL: http://xpt.sourceforge.net/
#
# CREDIT:
# Skeleton from: chris beggy chrisb@kippona.com
# Skeleton Id: skel-make-latex.el,v 1.3 2001/01/17 23:52:17 chrisb Exp 
#
# Readme begins:
# 
# This is a generic makefile to create output from a Latex file.
# 
# It will run the designated LaTeX FILE through TeX in turn until all
# cross-references are resolved, building all indices.  The directory
# containing each FILE is searched for included files.
# 
# It can also generate .html, .txt or .rtf files.
# 
# To get help, type 'make help'
# 
# Distribute freely, but please include the author's info & copyright,
# the file's version & url with the distribution.
# 
# Support free software movement! Please send you comments, suggestions, bug
# reports, patches to the author from the xpt project home URL. They are
# warmly welcome. Thank you for using the tools from the xpt project.
# 
# Readme ends.
#  cat Makefile | sed -n '/# [R]eadme /,/# [R]eadme / p' | cut -c3- > latexmake/README
# 
# {{{ Commentary:

#
#  To make the latex file latex_file.tex, issue:
#
#   make TEX_SOURCE_BASE=latex_file
#  or rather:
#   export TEX_SOURCE_BASE=latex_file
#   make
#  
# Basically you need to set the TEX_SOURCE_BASE variable before invoking make
# but using 'make check' can have 'make' makes suggestion for you: 
# If there is only one .tex file in current dirctory, make will suggest it.
# else suggest the directory name. For example, if the directory is named
# "thesis", make will assume that your will work on the file "thesis.tex".
# The benefit of setting this variable is that you can clean all the generated
# files easily using 'make clean'.

# If the TEX_DEST_DIR variable is set, all generated files (.ps, .pdf...) will
# end up in that particular directory. This is mandatory if you want to build
# an HTML (or .txt) version from the .tex file. The default is current dir.
#  
# It default makes only .dvi file, and no output unless errors occur
# You can also issue 'make view' to view the result right after the make
#
# To generate other formats, issue
#  
#   make ps
# and/or,
#   make pdf
#   make html
#   make txt
#   make rtf
#  
#  or simply:
#   make all rtf
#
# The general procedure would be
#   'make view' repeatly until you are fully satisfied
#   'make all', or 'make ps pdf html' for the final blast
#
#  Targets           Explanation
#  -------------------------------------------
#  dvi           dvi format
#  ps            postscript
#  pdf           pdf Adobe
#  html          for web browser
#  txt           normal plain text file
#  rtf           for M$ Word/WordPad
#
#  .tex          Latex source file 
#  .chk         check latex syntax with chktex
#
# The values of the BIBTEX, LATEX (or PDFLATEX), MAKEINDEX, MAKEINFO, TEX (or
# PDFTEX), and TEXINDEX environment variables are used to run those commands,
# if they are set.
#
# All tools used in this Makefile are well-known tools. They can all be easily
# located on the web using any search engines. Most of them may have already
# been included in your *nix distribution.
#

# }}} end commentary.
#
################################################

# {{{ Code:

# .............................................................. &ss ...
# here's the system dependent stuff
#

TEX_SOURCE_BASE=paper

ifndef SYNTAXCHK
SYNTAXCHK = chktex
endif

ifndef LATEX
LATEX = texi2dvi -q
endif

ifndef DVIPS
DVIPS=dvips
endif

ifndef DVIVIEWER
DVIVIEWER=xdvi
endif

ifndef PDFLATEX
PDFLATEX = pdflatex
PDFLATEX = texi2pdf -q -p
PDFLATEX = ps2pdf
endif

ifndef LATEX2HTML
LATEX2HTML=latex2html -verbosity 0 -split 0 -nonavigation -dir ${TEX_DEST_DIR}
endif

ifndef HTML2TEXT
HTML2TEXT=w3m -dump -cols 78 
endif

ifndef LATEX2RTF
LATEX2RTF=`which ltx2rtf`
endif

ifndef MAKEFILE
MAKEFILE=Makefile
endif

# .............................................................. &ss ...
# here are the default rules
#


# Disable standard pattern rule:
%.dvi: %.tex

# Do not delete the following targets:
.PRECIOUS: %.chk %.aux %.bbl

%.chk: %.tex
#	$(SYNTAXCHK) -o $@ $< 
	latex $<
	@touch $@

# .dvi file is always generated to the current dir, 
# regardless of the ${TEX_DEST_DIR} setting
%.dvi: %.tex %.chk
	$(LATEX) $<

%.ps : %.dvi %.chk
	$(DVIPS) -o $@ $< 
	@[ $${TEX_DEST_DIR:+T} ] && mv $@ ${TEX_DEST_DIR} || true

%.pdf : %.ps %.tex %.chk
	$(PDFLATEX) $<
	@[ $${TEX_DEST_DIR:+T} ] && mv $@ ${TEX_DEST_DIR} || true

${TEX_DEST_DIR}/%.html : %.tex %.chk
	$(LATEX2HTML) $<
	rm -f ${TEX_DEST_DIR}/index.html ${TEX_DEST_DIR}/labels.pl ${TEX_DEST_DIR}/WARNINGS
	@printf "\n\n"

${TEX_DEST_DIR}/%.txt : ${TEX_DEST_DIR}/%.html
	@echo $(HTML2TEXT) $<
	@cd ${TEX_DEST_DIR}; \
	$(HTML2TEXT) $< | perl \
	-pn000e 's{\n\s*\n\s*\n-}{\n-}g; s/\n/\r\n/g; s/\xa0//g; exit if /^About this document \./' > $@
#	sed "s/$$/`printf '\r'`/" > $@

%.rtf : %.tex %.chk
	$(LATEX2RTF) $<
	@[ $${TEX_DEST_DIR:+T} ] && mv $@ ${TEX_DEST_DIR} || true

# .............................................................. &ss ...
# here are the targets
#

# default is to build dvi
# it will be recreated if any .tex in current dir changed

dvi : $(TEX_SOURCE_BASE).chk $(TEX_SOURCE_BASE).dvi

$(TEX_SOURCE_BASE).dvi: $(wildcard *.tex)

view: $(TEX_SOURCE_BASE).dvi
	$(DVIVIEWER) ${TEX_SOURCE_BASE}.dvi &

# You can change the 'meaning' of 'all' the way you want
# by adding or deleting targets
all :  dvi ps pdf txt 		# txt implies html

ps : $(TEX_SOURCE_BASE).ps
	@printf " == The $@ version of the target generated\n\n\n"

pdf : $(TEX_SOURCE_BASE).pdf
	@printf " == The $@ version of the target generated\n\n\n"

html : ${TEX_DEST_DIR}/$(TEX_SOURCE_BASE).html 
	@printf " == The $@ version of the target generated\n\n\n"

${TEX_DEST_DIR}/$(TEX_SOURCE_BASE).html: $(TEX_SOURCE_BASE).tex

txt : ${TEX_DEST_DIR}/$(TEX_SOURCE_BASE).txt
	@printf " == The $@ version of the target generated\n\n\n"

rtf : $(TEX_SOURCE_BASE).rtf
	@printf " == The $@ version of the target generated\n\n\n"

# .............................................................. &ss ...
# here is the source
#
#TEX_SOURCE_BASE is get from environment or command line

check:
# Make sure the TEX_SOURCE_BASE is set and help set it if not.
	@if [ $${TEX_SOURCE_BASE:+T} ]; then \
	  printf "TEX_SOURCE_BASE=${TEX_SOURCE_BASE}\nTEX_DEST_DIR=${TEX_DEST_DIR}\n"; \
	else  { \
		if [ `ls *.tex | wc -l` = "1" ]; then \
			TEX_SOURCE_BASE=`basename \`ls *.tex\` .tex`; \
			true; \
		else \
			TEX_SOURCE_BASE=`echo $$PWD|tr '/' '\n'|tail -1`; \
			true; \
		fi; \
	printf "\nPlease set\n\n export TEX_SOURCE_BASE=$$TEX_SOURCE_BASE\nor,\n setenv TEX_SOURCE_BASE $$TEX_SOURCE_BASE\n"; \
	}; fi

# .............................................................. &ss ...
.PHONY : help usage check clean cleangen clean-bak clean-th

help:
	@echo "To see the usage of this make file, type 'make usage'"
	@echo " (need to set the environment var MAKEFILE if you "
	@echo "  have change the default name/location of this Makefile"
	@echo "  which requires the -f parameter for the make)"

usage:
	@cat ${MAKEFILE} | sed -n '/ [Cc]ommentary/,/ [Cc]ommentary/ p' | cut -c3-
	@echo 
	@echo To get the help from the tools used:
	@echo type \'texi2pdf --help\'
#	texi2pdf --help

clean: 
	rm -f ${TEX_SOURCE_BASE}.chk ${TEX_SOURCE_BASE}.dvi ${TEX_SOURCE_BASE}.log ${TEX_SOURCE_BASE}.aux ${TEX_SOURCE_BASE}.bbl ${TEX_SOURCE_BASE}.blg ${TEX_SOURCE_BASE}.ilg ${TEX_SOURCE_BASE}.toc ${TEX_SOURCE_BASE}.lof ${TEX_SOURCE_BASE}.lot ${TEX_SOURCE_BASE}.idx ${TEX_SOURCE_BASE}.ind ${TEX_SOURCE_BASE}.out

cleangen :
	rm -f *.chk *.dvi *.log *.aux *.bbl *.blg *.ilg *.toc *.lof *.lot *.idx *.ind *.out *.ps *.pdf  

clean-bak : clean
	rm -f *~

clean-th : clean
	rm -f *.txt *.html *.css

# .............................................................. &ss ...

# }}}

## end of Makefile
