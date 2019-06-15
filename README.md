Plan 

let kv = Cavey::new("/path/to/kv");
kv.put(String::from("x"), (14).into())?;
kv.put(String::from("y"), [1, 2, 3].into())?;
let val = kv.get("x")?.as_int();


On init:

Store receives messages on a channel.  For each message, check the type, and 
handle appropriately.
