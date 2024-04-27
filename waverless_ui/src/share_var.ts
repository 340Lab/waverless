var _share_var: any = undefined

export function share_var() {
    if (_share_var === undefined) {
        _share_var = {
            // export var service_basic_list = [];
            service_basic_showing: undefined,
            each_service_thing: {} as any,
        }
        _share_var.set_service_recent_actions = (service: string, actions: any) => {
            if (service in _share_var.each_service_thing === false) {
                _share_var.each_service_thing[service] = {
                    recent_actions: []
                }
            }
            _share_var.each_service_thing[service].recent_actions = actions
        }
        _share_var.get_service_recent_actions = (service: string) => {
            if (_share_var.each_service_thing[service] === undefined || ("recent_actions" in _share_var.each_service_thing[service] === false)) {
                return []
            }
            return _share_var.each_service_thing[service].recent_actions
        }
    }
    return _share_var
}