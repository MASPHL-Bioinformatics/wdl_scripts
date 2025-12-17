version 1.0

workflow sphl_lims_prep {
  meta {
    description: "Takes output from Theiagens TheiaCoV_Illumina_PE_PHB and aggregates for LIMS integration"
  }
  input {
    String    samplename
    Float     percent_reference_coverage
    String    meanbaseq
    String    meanmapq
    String    pango_lineage
    String    pangolin_version
    String    analysis_method
    String    analysis_version
    String    batch_id
    Float     cov_threshold
    Int       qc_reads_raw
    Int       qc_reads_clean
    Float     kraken_human
    Float     kraken_sc2
    Float     kraken_human_dehosted
    Float     kraken_sc2_dehosted
    Int       number_N
    Int       assembly_length_unambiguous
    Int       number_Degenerate
    Int       number_Total
    Float     percent_reference_coverage
    String    assembly_mean_coverage
    String    nextclade_aa_subs
    String    nextclade_aa_dels
    String    nextclade_clade
    String    utiltiy_docker  = "quay.io/broadinstitute/viral-baseimage@sha256:340c0a673e03284212f539881d8e0fb5146b83878cbf94e4631e8393d4bc6753"
  }
  call lims_prep {
    input:
      samplename                 = samplename,
      percent_reference_coverage = select_first([percent_reference_coverage, 0.0]), 
      meanbaseq                  = meanbaseq, 
      meanmapq                   = meanmapq,
      pango_lineage              = select_first([pango_lineage, "NA"]),
      cov_threshold              = cov_threshold,
      docker                     = utiltiy_docker
  }  
  output {
    String    assembly_status         = lims_prep.assembly_status
    String    report_tool_lineage     = lims_prep.tool_lineage
    String    report_lineage_to_maven = lims_prep.tool_lineage
    String    report_pango_version    = select_first([pangolin_version, "NA"])
    String    report_organism         = "SARS-CoV-2"
    String    report_test             = "SARS-CoV-2 Sequencing"
    String    report_method           = analysis_method
    String    report_method_version   = analysis_version
    String    batchid                 = batch_id
    String    report_meanbaseq                    = select_first([meanbaseq, "NA"])
    String    report_meanmapq                     = select_first([meanmapq, "NA"])
    String    report_pango_lineage                = select_first([pango_lineage, "NA"])
    Int       report_qc_reads_raw                 = select_first([qc_reads_raw, 0])
    Int       report_qc_reads_clean               = select_first([qc_reads_clean, 0])
    Float     report_kraken_human                 = select_first([kraken_human, 0.0])
    Float     report_kraken_sc2                   = select_first([kraken_sc2, 0.0])
    Float     report_kraken_human_dehosted        = select_first([kraken_human_dehosted, 0.0])
    Float     report_kraken_sc2_dehosted          = select_first([kraken_sc2_dehosted, 0.0])
    Int       report_number_N                     = select_first([number_N, 0])
    Int       report_assembly_length_unambiguous  = select_first([assembly_length_unambiguous, 0])
    Int       report_number_Degenerate            = select_first([number_Degenerate, 0])
    Int       report_number_Total                 = select_first([number_Total, 0])
    Float     report_percent_reference_coverage   = select_first([percent_reference_coverage, 0.0])
    String    report_assembly_mean_coverage       = select_first([assembly_mean_coverage, "NA"])
    String    report_nextclade_aa_subs            = select_first([nextclade_aa_subs, "NA"])
    String    report_nextclade_aa_dels            = select_first([nextclade_aa_dels, "NA"])
    String    report_nextclade_clade              = select_first([nextclade_clade, "NA"])
  }
}

task lims_prep {
  input {
    String    samplename
    Float     percent_reference_coverage
    String    meanbaseq
    String    meanmapq
    String    pango_lineage
    Float     cov_threshold
    String    docker
  }
  command <<<
    python3 <<CODE

    if "~{meanbaseq}" != "" and "~{meanmapq}" != "":
      meanbaseq = float(~{meanbaseq})
      meanmapq = float(~{meanmapq})
    else:
      meanbaseq = 0.0
      meanmapq = 0.0

    if ~{percent_reference_coverage} >= ~{cov_threshold} and meanbaseq >= 20 and meanmapq >= 20:
      with open("STATUS", 'wt') as thing: thing.write("PASS")
      with open("TOOL_LIN", 'wt') as thing: thing.write("~{pango_lineage}")
      with open("MAV_LIN", 'wt') as thing: thing.write("~{pango_lineage}")
    elif ~{percent_reference_coverage} < ~{cov_threshold} or meanbaseq < 20 or meanmapq < 20:
      with open("STATUS", 'wt') as thing: thing.write("FAIL")
      with open("TOOL_LIN", 'wt') as thing: thing.write("INVALID")
      with open("MAV_LIN", 'wt') as thing: thing.write("INVALID")
    else:
      with open("STATUS", 'wt') as thing: thing.write("UNKNOWN")
      with open("TOOL_LIN", 'wt') as thing: thing.write("UNKNOWN")
      with open("MAV_LIN", 'wt') as thing: thing.write("UNKNOWN")
    CODE
  >>>
  output {
    String    assembly_status  = read_string("STATUS")
    String    tool_lineage     = read_string("TOOL_LIN")
    String    lineage_to_maven = read_string("MAV_LIN")
  }
  runtime {
    docker: docker
    memory: "1 GB"
    cpu: 1
  }
}
