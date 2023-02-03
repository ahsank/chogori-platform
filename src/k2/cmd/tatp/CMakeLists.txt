file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (tatp_client ${HEADERS} ${SOURCES})

target_link_libraries (tatp_client PRIVATE tso_client appbase transport common cpo_client k23si_client dto Seastar::seastar)

install (TARGETS tatp_client DESTINATION bin)
