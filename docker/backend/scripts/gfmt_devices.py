import json
from NovaApi.ListDevices.nbe_list_devices import request_device_list
from ProtoDecoders.decoder import parse_device_list_protobuf, get_canonic_ids

def main():
    hex_resp = request_device_list()
    device_list = parse_device_list_protobuf(hex_resp)
    ids = get_canonic_ids(device_list)
    # ids is list of (name, canonic_id)
    out = [{"name": name, "canonic_id": cid} for name, cid in ids]
    print(json.dumps(out, ensure_ascii=False))

if __name__ == "__main__":
    main()
