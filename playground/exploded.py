import ipaddress

def main():
    ip_txt = "2607:5300:5432:abcd::25b5"
    ip_obj = ipaddress.IPv6Address(ip_txt)
    print(f"{type(ip_obj)}")
    full = ip_obj.exploded.replace(":", "")
    print(f"full form {full}")

    prefix_len = 48
    network = ipaddress.IPv6Network(f"{ip_txt}/{prefix_len}", strict=False)
    print(f"subnet prefix: {network.network_address}")
    prefix_len = 50
    network = ipaddress.IPv6Network(f"{ip_txt}/{prefix_len}", strict=False)
    print(f"subnet prefix: {network.network_address}")

if __name__ == "__main__":
    main()