version 1.0

workflow sphl_lims_prep {
  meta {
    description: "Takes output from Theiagens TheiaCoV_Illumina_PE_PHB and aggregates for LIMS integration"
  }
  input {
    String    samplename
    Float?     percent_reference_coverage
    Float?     meanbaseq
    Float?     meanmapq
    String?    pango_lineage
    String?    pangolin_version
    String    analysis_method
    String    analysis_version
    String    batch_id
    Float     cov_threshold
    String    utiltiy_docker  = "quay.io/broadinstitute/viral-baseimage@sha256:340c0a673e03284212f539881d8e0fb5146b83878cbf94e4631e8393d4bc6753"
  }
  call lims_prep {
    input:
      samplename                 = samplename,
      percent_reference_coverage = select_first([percent_reference_coverage, 0.0]), 
      meanbaseq                  = select_first([meanbaseq, 0.0]), 
      meanmapq                   = select_first([meanmapq, 0.0]),
      pango_lineage              = select_first([pango_lineage, "NA"]),
      cov_threshold              = cov_threshold,
      docker                     = utiltiy_docker
  }  
  output {
    String    assembly_status  = lims_prep.assembly_status
    String    tool_lineage     = lims_prep.tool_lineage
    String    lineage_to_maven = lims_prep.tool_lineage
    String    pango_version    = select_first([pangolin_version, "NA"])
    String    organism         = "SARS-CoV-2"
    String    test             = "SARS-CoV-2 Sequencing"
    String    method           = analysis_method
    String    method_version   = analysis_version
    String    batchid          = batch_id
  }
}

task lims_prep {
  input {
    String    samplename
    Float     percent_reference_coverage
    Float     meanbaseq
    Float     meanmapq
    String    pango_lineage
    Float     cov_threshold
    String    docker
  }
  command <<<
    python3 <<CODE
    if ~{percent_reference_coverage} >= ~{cov_threshold} and ~{meanbaseq} >= 20 and ~{meanmapq} >= 20:
      with open("STATUS", 'wt') as thing: thing.write("PASS")
      with open("TOOL_LIN", 'wt') as thing: thing.write("~{pango_lineage}")
      with open("MAV_LIN", 'wt') as thing: thing.write("~{pango_lineage}")
    elif ~{percent_reference_coverage} < ~{cov_threshold} or ~{meanbaseq} < 20 or ~{meanmapq} < 20:
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
