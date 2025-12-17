version 1.0

workflow sphl_lims_file_gen {
  meta {
    description: "Takes output from Theiagens TheiaCoV_Illumina_PE_PHB and aggregates for LIMS integration"
  }
  input {
    Array[String]    samplename
    Array[String]    batchid
    Array[String]    seqdate
    Array[String]    assembly_status
    Array[String]?   qc_reads_raw 
    Array[String]?   qc_reads_clean 
    Array[String]?   kraken_human 
    Array[String]?   kraken_sc2 
    Array[String]?   kraken_human_dehosted 
    Array[String]?   kraken_sc2_dehosted 
    Array[String]?   number_N
    Array[String]?   assembly_length_unambiguous 
    Array[String]?   number_Degenerate
    Array[String]?   number_Total
    Array[String]?   percent_reference_coverage
    Array[String]?   meanbaseq_trim
    Array[String]?   meanmapq_trim
    Array[String]?   assembly_mean_coverage
    Array[String]    tool_lineage
    Array[String]?   pango_lineage
    Array[String]    pango_version
    Array[String]?   nextclade_aa_subs
    Array[String]?   nextclade_aa_dels
    Array[String]?   nextclade_clade
    Array[String]    lineage_to_maven
    String           organism = "SARS-CoV 2"
    String           test = "SARS-CoV-2 Sequencing"
    String           utility_docker  = "quay.io/broadinstitute/viral-baseimage@sha256:340c0a673e03284212f539881d8e0fb5146b83878cbf94e4631e8393d4bc6753"
  }
  call lims_file_gen {
    input:
      samplename       = samplename,
      assembly_status  = assembly_status, 
      tool_lineage     = tool_lineage,
      lineage_to_maven = lineage_to_maven,
      pango_version    = pango_version,
      organism         = organism,
      test             = test,
      docker           = utility_docker
  }  
  call run_results_file_gen {
    input:
      samplename                  = samplename,
      batchid                     = batchid,
      seqdate                     = seqdate,
      assembly_status             = assembly_status,
      pango_lineage               = pango_lineage,
      qc_reads_raw                  = qc_reads_raw,
      qc_reads_clean                = qc_reads_clean,
      kraken_human                = kraken_human,
      kraken_sc2                  = kraken_sc2,
      kraken_human_dehosted       = kraken_human_dehosted, 
      kraken_sc2_dehosted         = kraken_sc2_dehosted,
      number_N                    = number_N,
      assembly_length_unambiguous = assembly_length_unambiguous,
      number_Degenerate           = number_Degenerate,
      number_Total                = number_Total,
      percent_reference_coverage  = percent_reference_coverage,
      meanbaseq_trim              = meanbaseq_trim,
      meanmapq_trim               = meanmapq_trim,
      assembly_mean_coverage      = assembly_mean_coverage,
      nextclade_aa_subs           = nextclade_aa_subs,
      nextclade_aa_dels           = nextclade_aa_dels,
      nextclade_clade             = nextclade_clade,
      pango_version               = pango_version,
      docker                      = utility_docker
  }
  output {
    File      btb_lims_file = lims_file_gen.lims_file
    File      run_results_file = run_results_file_gen.results_file
  }
}

task lims_file_gen {
  input {
    Array[String]    samplename
    Array[String]    assembly_status
    Array[String]    tool_lineage
    Array[String]    lineage_to_maven
    Array[String]    pango_version
    String           organism
    String           test
    String           docker
  }
  command <<<
    python3 <<CODE
    samplename_array=['~{sep="','" samplename}']
    samplename_array_len=len(samplename_array)
    assembly_status_array=['~{sep="','" assembly_status}']
    assembly_status_array_len=len(assembly_status_array)
    tool_lineage_array=['~{sep="','" tool_lineage}']
    tool_lineage_array_len=len(tool_lineage_array)
    lineage_maven_array=['~{sep="','" lineage_to_maven}']
    lineage_maven_array_len=len(lineage_maven_array)
    pango_version_array=['~{sep="','" pango_version}']
    pango_version_array_len=len(pango_version_array)

    from datetime import datetime, timezone, timedelta
    outfile = open(f'{datetime.now(timezone(timedelta(hours=-4))).strftime("%Y-%m-%d")}.lims_file.csv', 'w')
    if samplename_array_len == assembly_status_array_len == tool_lineage_array_len == lineage_maven_array_len == pango_version_array_len:
      outfile.write('sample_id,assembly_status,tool_lineage,lineage_to_maven,pango_version,organism,test\n')
      index = 0
      print(f'Index:{index}\tSamplename:{samplename_array_len}')
      while index < samplename_array_len:
        print(f'Processing for index {index}')
        name = samplename_array[index]
        status = assembly_status_array[index]
        lineage = tool_lineage_array[index]
        lineage_maven = lineage_maven_array[index]
        pango = pango_version_array[index]
        outfile.write(f'{name},{status},{lineage},{lineage_maven},{pango},~{organism},~{test}\n')
        index += 1
    else: 
      print(f'Input arrays are of unequal length. (Samplename:{samplename_array_len}, Status:{assembly_status_array_len}, Tool Lineage:{tool_lineage_array_len})')
      outfile.write(f'Input arrays are of unequal length. (Samplename:{samplename_array_len}, Status:{assembly_status_array_len}, Tool Lineage:{tool_lineage_array_len})')
    CODE
  >>>
  output {
    File    lims_file = select_first(glob('*lims_file.csv'))
  }
  runtime {
    docker: docker
    memory: "1 GB"
    cpu: 1
  }
}


task run_results_file_gen {
  input {
    Array[String]    samplename
    Array[String]    batchid
    Array[String]    seqdate
    Array[String]    assembly_status
    Array[String]?    pango_lineage
    Array[String]?     qc_reads_raw
    Array[String]?     qc_reads_clean
    Array[String]?     kraken_human
    Array[String]?     kraken_sc2
    Array[String]?     kraken_human_dehosted
    Array[String]?     kraken_sc2_dehosted
    Array[String]?     number_N
    Array[String]?     assembly_length_unambiguous
    Array[String]?     number_Degenerate
    Array[String]?     number_Total
    Array[String]?     percent_reference_coverage
    Array[String]?     meanbaseq_trim
    Array[String]?     meanmapq_trim
    Array[String]?     assembly_mean_coverage
    Array[String]?    nextclade_aa_subs
    Array[String]?    nextclade_aa_dels
    Array[String]?    nextclade_clade
    Array[String]    pango_version
    String           docker
  }
  command <<<
    python3 <<CODE
    samplename_array=['~{sep="','" samplename}']
    batchid_array=['~{sep="','" batchid}']
    seq_date_array=['~{sep="','" seqdate}']
    assembly_status_array=['~{sep="','" assembly_status}']
    percent_reference_coverage_array=['~{sep="','" percent_reference_coverage}']
    assembly_mean_coverage_array=['~{sep="','" assembly_mean_coverage}']
    meanbaseq_trim_array=['~{sep="','" meanbaseq_trim}']
    meanmapq_trim_array=['~{sep="','" meanmapq_trim}']
    nextclade_clade_array=['~{sep="','" nextclade_clade}']
    pango_lineage_array=['~{sep="','" pango_lineage}']
    qc_reads_raw_array=['~{sep="','" qc_reads_raw}']
    qc_reads_clean_array=['~{sep="','" qc_reads_clean}']
    kraken_human_array=['~{sep="','" kraken_human}']
    kraken_sc2_array=['~{sep="','" kraken_sc2}']
    kraken_human_dehosted_array=['~{sep="','" kraken_human_dehosted}']
    kraken_sc2_dehosted_array=['~{sep="','" kraken_sc2_dehosted}']
    number_N_array=['~{sep="','" number_N}']
    number_Degenerate_array=['~{sep="','" number_Degenerate}']
    assembly_length_unambiguous_array=['~{sep="','" assembly_length_unambiguous}']
    number_Total_array=['~{sep="','" number_Total}']
    pango_version_array=['~{sep="','" pango_version}']
    nextclade_aa_subs_array=['~{sep="','" nextclade_aa_subs}']
    nextclade_aa_dels_array=['~{sep="','" nextclade_aa_dels}']

    fields = [batchid_array,assembly_status_array,percent_reference_coverage_array,assembly_mean_coverage_array,meanbaseq_trim_array,meanmapq_trim_array,nextclade_clade_array,pango_lineage_array,qc_reads_raw_array,qc_reads_clean_array,kraken_human_array,kraken_sc2_array,kraken_human_dehosted_array,kraken_sc2_dehosted_array,number_N_array,number_Degenerate_array,assembly_length_unambiguous_array,number_Total_array,pango_version_array,nextclade_aa_subs_array,nextclade_aa_dels_array]

    # count number of elements in each list. If not all equal, will not populate into table. 
    unequal = 0
    print(f'samplename_array : {len(samplename_array)}')
    for field in fields:
      print(f'{len(field)} : {field}')
      if len(field) != len(samplename_array):
        unequal += 1

    print(f'Number unequal to samplename_array {unequal}')
    from datetime import datetime, timezone, timedelta
    outfile = open(f'{datetime.now(timezone(timedelta(hours=-4))).strftime("%Y-%m-%d")}.run_results.csv', 'w')
    if unequal == 0:
      outfile.write('sample_id,batch_id,seq_date,assembly_status,percent_reference_coverage,mean_depth,meanbaseq_trim,meanmapq_trim,nextclade_lineage,pangolin_lineage,qc_reads_raw,qc_reads_clean,%_human_reads,%_SARS-COV-2_reads,dehosted_%human,dehosted_%SC2,num_N,num_degenerate,num_ACTG,num_total,pangolin_version,AA_substitutions,AA_deletions\n')

      index = 0
      while index < len(samplename_array):
        samplename = "NA" if samplename_array[index] == "" else samplename_array[index]
        batchid = "NA" if batchid_array[index]== "" else batchid_array[index]
        seq_date = "NA" if seq_date_array[index]== "" else seq_date_array[index]
        assembly_status = "NA" if assembly_status_array[index]== "" else assembly_status_array[index]
        percent_reference_coverage = "NA" if percent_reference_coverage_array[index]== "" else percent_reference_coverage_array[index]
        assembly_mean_coverage = "NA" if assembly_mean_coverage_array[index]== "" else assembly_mean_coverage_array[index]
        meanbaseq_trim = "NA" if meanbaseq_trim_array[index]== "" else meanbaseq_trim_array[index]
        meanmapq_trim = "NA" if meanmapq_trim_array[index]== "" else meanmapq_trim_array[index]
        nextclade_clade = "NA" if nextclade_clade_array[index].replace(',','')== "" else nextclade_clade_array[index].replace(',','')
        pango_lineage = "NA" if pango_lineage_array[index]== "" else pango_lineage_array[index]
        qc_reads_raw = "NA" if qc_reads_raw_array[index]== "" else qc_reads_raw_array[index]
        qc_reads_clean = "NA" if qc_reads_clean_array[index]== "" else qc_reads_clean_array[index]
        kraken_human = "NA" if kraken_human_array[index]== "" else kraken_human_array[index]
        kraken_sc2 = "NA" if kraken_sc2_array[index]== "" else kraken_sc2_array[index]
        kraken_human_dehosted = "NA" if kraken_human_dehosted_array[index]== "" else kraken_human_dehosted_array[index]
        kraken_sc2_dehosted = "NA" if kraken_sc2_dehosted_array[index]== "" else kraken_sc2_dehosted_array[index]
        number_N = "NA" if number_N_array[index]== "" else number_N_array[index]
        number_Degenerate = "NA" if number_Degenerate_array[index]== "" else number_Degenerate_array[index]
        assembly_length_unambiguous = "NA" if assembly_length_unambiguous_array[index]== "" else assembly_length_unambiguous_array[index]
        number_Total = "NA" if number_Total_array[index]== "" else number_Total_array[index]
        pango_version = "NA" if pango_version_array[index]== "" else pango_version_array[index]
        nextclade_aa_subs = "NA" if nextclade_aa_subs_array[index].replace(',','|')== "" else nextclade_aa_subs_array[index].replace(',','|')
        nextclade_aa_dels = "NA" if nextclade_aa_dels_array[index].replace(',','|')== "" else nextclade_aa_dels_array[index].replace(',','|')
        outfile.write(f'{samplename},{batchid},{seq_date},{assembly_status},{percent_reference_coverage},{assembly_mean_coverage},{meanbaseq_trim},{meanmapq_trim},{nextclade_clade},{pango_lineage},{qc_reads_raw},{qc_reads_clean},{kraken_human},{kraken_sc2},{kraken_human_dehosted},{kraken_sc2_dehosted},{number_N},{number_Degenerate},{assembly_length_unambiguous},{number_Total},{pango_version},{nextclade_aa_subs},{nextclade_aa_dels},\n')
        index += 1
    else: 
      print(f'Input arrays are of unequal length.')
      outfile.write(f'Input arrays are of unequal length.\n')
      outfile.write(f'{len(samplename_array)}:\t{samplename_array}')
      for field in fields:
        outfile.write(f'{len(field)}:\t{field}\n')
    CODE
  >>>
  output {
    File    results_file = select_first(glob('*run_results.csv'))
  }
  runtime {
    docker: docker
    memory: "1 GB"
    cpu: 1
  }
}