/*
 * checkpoint.thrift
 */

namespace java com.nexr.cp.thrift

service CheckPointService {
	list<string> checkTagId(1:string agentName)
}