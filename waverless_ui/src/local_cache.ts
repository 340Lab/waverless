export namespace local_cache {
    let cache_meta: any = undefined
    export function cache(key: string, data: string) {
        // if (!cache_meta) {
        //     cache_meta = localStorage.getItem('cache_meta')
        // }
        // if (!cache_meta) {
        //     cache_meta = {}
        // }
        // localStorage.setItem('cache_meta', cache_meta)
        // localStorage.setItem('_' + key, data)
    }

    export function try_get(key: string) {
        return localStorage.getItem('_' + key)
    }
}