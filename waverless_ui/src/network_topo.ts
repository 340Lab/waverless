export class UINode {
    constructor(
        public x: number,
        public y: number,
        public zIndex: number,
        public id: number,
        public index: number
    ) { }
}

export class UILink {
    constructor(
        public source: [number, number],
        public bandwidth: number
    ) { }
}

export class Topo {
    constructor(
        public nodes: UINode[],
        public links: Map<string, UILink>
    ) { }
}
