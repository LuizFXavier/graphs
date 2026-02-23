#include <iostream>
#include <cstdint>
#include <memory>
#include <stdexcept>
import mna.io.cli_parser;
import mna.io.directory_scanner;
import mna.io.parquet_reader;
import mna.model.iot_network;
import mna.model.job;
import dimna.allocator.di_allocator;

int
main (int argc, char *argv[]) {
  mna::Config dir_config = mna::read_CLI(argc, argv);

  mna::InstancesMap instances = mna::scan_input_dir(dir_config.input_folder);
  
  mna::InstanceStructure lastInstance;

  for (const auto& [key, value] : instances)
    lastInstance = value;
  
  mna::ParquetReader pr;

  int total_vertexes = *(pr.get_num_rows(lastInstance.nodes));
  int total_edges = *(pr.get_num_rows(lastInstance.edges[0]));

  auto network = std::make_shared<mna::IoTNetwork>(total_vertexes);

  using vertex_type = decltype(network)::element_type::vertex_type;
  using edge_type = decltype(network)::element_type::edge_type;

  auto add_edge = [&](int32_t source, int32_t target, int32_t latency){
    network->add_edge(edge_type{target, latency}, source); 
  };
  auto add_vertex = [&](int64_t R, int64_t B, int64_t busy, int64_t inactive){
    network->add_vertex(vertex_type{R, B, busy, inactive});
  };

  mna::JobVector jobsV;

  auto add_job = [&](int64_t jr, int64_t jb, int64_t jl, int64_t jo){
    std::cout << jr << " " << jb << " " << jl << " " << jo << "\n";
    jobsV.push_back(mna::Job{jr, jb, jl, jo});
  };

  pr.read_vertexes(lastInstance.nodes, add_vertex);
  pr.read_edges(lastInstance.edges, add_edge);
  pr.read_jobs(lastInstance.jobs[0], add_job);

  mna::di::DiAllocator allocator(network, 1, 1);

  for (auto [t, w] : network->get_node_edges(jobsV[0].origin)){
    std::cout << "wa: " << w << " t:" << t << "\n";
  }

  auto latencies = allocator.get_latencies(jobsV[0].origin);

  int validation[] = {65, 25, 74, 31, 26, 77, 34, 48, 0, 21};

  for (int i = 0; i < latencies.size(); ++i){
    if(latencies[i] != validation[i])
      throw std::runtime_error("Error: Mismatch on obtained latencies");
  }
  // std::cout << "\n" << jobsV.size() << "\n";
  return 0;
}
