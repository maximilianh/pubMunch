The hugo.tab file can be updated like this:

wget "http://www.genenames.org/cgi-bin/hgnc_downloads.cgi?title=HGNC+output+data&hgnc_dbtag=onlevel=pri&=on&order_by=gd_app_sym_sort&limit=&format=text&.cgifields=&.cgifields=level&.cgifields=chr&.cgifields=status&.cgifields=hgnc_dbtag&&status=Approved&status=Entry+Withdrawn&status_opt=2&submit=submit&col=gd_hgnc_id&col=gd_app_sym&col=gd_app_name&col=gd_status&col=gd_prev_sym&col=gd_aliases&col=gd_pub_chrom_map&col=gd_pub_acc_ids&col=gd_pub_refseq_ids&where=gd_locus_group+%3D+'protein-coding%20gene'" -O - |  cut -f1,2,,6 | grep -v 'Approved Symbol' > hugo.tab

the species-frequency.tsv.gz is from linnaeus, slightly reformatted. 
