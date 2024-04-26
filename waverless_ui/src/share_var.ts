var _share_var: any = undefined

export function share_var() {
    if (_share_var === undefined) {
        _share_var = {
            // export var service_basic_list = [];
            service_basic_showing: undefined
        }
    }
    return _share_var
}