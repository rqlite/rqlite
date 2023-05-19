package tcp

import (
	"net"
)

// NetworkReporter is a reporter for network information
type NetworkReporter struct{}

// Address is a struct for holding network addresses
type Address struct {
	Addr string `json:"address"`
}

// InterfaceDetail is a struct for holding network interface details
type InterfaceDetail struct {
	Flags           string    `json:"flags"`
	HardwareAddress string    `json:"hardware_address"`
	Addresses       []Address `json:"addresses"`
}

// InterfaceStats is a map of network interface names to InterfaceDetail
type InterfaceStats map[string]InterfaceDetail

// Stats returns network interface details
func (n NetworkReporter) Stats() (map[string]interface{}, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	stats := make(map[string]interface{})
	for _, i := range interfaces {
		var addresses []Address

		addrs, err := i.Addrs()
		if err != nil {
			return nil, err
		}

		for _, addr := range addrs {
			addresses = append(addresses, Address{Addr: addr.String()})
		}

		stats[i.Name] = InterfaceDetail{
			Flags:           i.Flags.String(),
			HardwareAddress: i.HardwareAddr.String(),
			Addresses:       addresses,
		}
	}

	return stats, nil
}
