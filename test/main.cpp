import std;
import graphs;
import test.graph;
using namespace graphs;

int
main(){
  using VC = VertexVector<test::Vertex>;
  using EC = EdgeAdjacencyList<test::Edge>;

  Graph<VC, EC> graph(2);

  graph.add_vertex(test::Vertex{2});
  graph.add_edge(test::Edge{1, 3}, 0);
  std::cout << "Compila!\n";
  return 0;
}
