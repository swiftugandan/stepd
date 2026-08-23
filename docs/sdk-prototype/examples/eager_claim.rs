//! Does claiming the hash EAGERLY (at call time) rather than lazily (at poll time)
//! make `join!` safe automatically? If so, the type system plus eager claiming
//! removes the whole hazard class, and the runtime token check is only a backstop.
use std::cell::RefCell;
use std::collections::HashMap;

struct Ctx { counters: RefCell<HashMap<String,u32>>, log: RefCell<Vec<String>> }

impl Ctx {
    fn new() -> Self { Ctx{counters:RefCell::new(HashMap::new()), log:RefCell::new(vec![])} }

    /// EAGER: the occurrence is claimed when this fn is CALLED, in program order.
    /// The returned future carries an already-fixed hash.
    fn step<'a>(&'a self, id: &'a str) -> impl std::future::Future<Output=String> + 'a {
        let occ = { let mut c=self.counters.borrow_mut(); let n=c.entry(id.into()).or_insert(0); let o=*n; *n+=1; o };
        let hash = format!("{id}#{occ}");
        self.log.borrow_mut().push(format!("claimed {hash}"));
        async move {
            // simulate work completing in an arbitrary order
            hash
        }
    }
}

fn main() {
    // Case 1: join! — futures created in program order, polled in arbitrary order.
    let ctx = Ctx::new();
    let f1 = ctx.step("a");
    let f2 = ctx.step("b");
    let f3 = ctx.step("a");           // second occurrence of "a"
    // Poll them in REVERSE order to prove polling order is irrelevant.
    let r = futures::executor::block_on(async { futures::join!(f3, f2, f1) });
    println!("join! results (polled reverse): {:?}", r);
    println!("claim log (program order):      {:?}", ctx.log.borrow());

    // Case 2: same handler shape, different poll order — hashes must be identical.
    let ctx2 = Ctx::new();
    let g1 = ctx2.step("a");
    let g2 = ctx2.step("b");
    let g3 = ctx2.step("a");
    let r2 = futures::executor::block_on(async { futures::join!(g1, g2, g3) });
    println!("join! results (polled forward): {:?}", (r2.2.clone(), r2.1.clone(), r2.0.clone()));

    assert_eq!(r.0, r2.2, "hash for third-declared step identical regardless of poll order");
    assert_eq!(r.2, r2.0, "hash for first-declared step identical regardless of poll order");
    println!("\nEAGER CLAIMING MAKES join! SAFE: hashes follow declaration order, not poll order.");

    // Case 3: what a LAZY claim would do (claim inside the async block).
    let ctx3 = Ctx::new();
    let lazy = |id: &'static str| {
        let c = &ctx3;
        async move {
            let occ = { let mut m=c.counters.borrow_mut(); let n=m.entry(id.into()).or_insert(0); let o=*n; *n+=1; o };
            format!("{id}#{occ}")
        }
    };
    let l1 = lazy("a"); let l2 = lazy("a");
    let lr = futures::executor::block_on(async { futures::join!(l2, l1) }); // reversed poll
    println!("\nLAZY claim, polled reverse: {:?}  <-- occurrence follows POLL order", lr);
    println!("LAZY is the bug: the same source line gets a different hash depending on scheduling.");
}
