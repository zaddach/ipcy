#include <cstddef>
#include <cstdio>
#include "ipcz/fragment_descriptor.h"
#include "ipcz/node_name.h"
#include "ipcz/ref_counted_fragment.h"
#include "ipcz/router_link_state.h"
using namespace ipcz;
#define P(T) std::printf("%s size=%zu align=%zu\n", #T, sizeof(T), alignof(T))
#define O(T, f) std::printf("%s.%s offset=%zu\n", #T, #f, offsetof(T, f))
int main() {
  P(NodeName); P(FragmentDescriptor); P(RefCountedFragment); P(RouterLinkState);
  O(RouterLinkState, status); O(RouterLinkState, allowed_bypass_request_source); O(RouterLinkState, reserved1);
}
