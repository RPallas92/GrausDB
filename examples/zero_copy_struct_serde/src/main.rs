use graus_db::GrausDb;
use musli_zerocopy::{endian, Buf, OwnedBuf, Ref, ZeroCopy};
use std::error::Error;
use std::mem;

/// Represents a product with a stock count and a name.
/// `#[derive(ZeroCopy)]` enables zero-copy serialization/deserialization with `musli-zerocopy`.
/// `#[repr(C)]` ensures a C-compatible memory layout, which is required for zero-copy.
#[derive(ZeroCopy)]
#[repr(C)]
struct Product {
    stock: u16,
    name: Ref<str>, // `Ref<str>` allows zero-copy referencing of string data within the buffer.
}

impl Product {
    /// Serializes a `Product` into an `OwnedBuf` using zero-copy principles.
    /// The `stock` is stored directly, and the `name_str` is stored as a `Ref<str>`.
    /// This avoids copying the string data during serialization.
    fn to_bytes(stock: u16, name_str: &str) -> OwnedBuf {
        // Create a new owned buffer, configured for little-endian byte order.
        let mut buf = OwnedBuf::new().with_byte_order::<endian::Little>();
        // Reserve space for the `Product` struct without initializing it.
        let product_ref = buf.store_uninit::<Product>();
        // Store the string data for the name, returning a `Ref<str>` to it within the buffer.
        let name = buf.store_unsized(name_str);
        // Load the uninitialized `Product` reference and write the actual `Product` data into it.
        buf.load_uninit_mut(product_ref)
            .write(&Product { stock, name });
        buf
    }

    /// Deserializes a `Product` reference from a byte slice using zero-copy.
    /// This function returns a reference to the `Product` directly from the input bytes,
    /// without allocating new memory for the struct itself.
    fn from_bytes<'a>(bytes: &'a [u8]) -> &'a Product {
        // Create a `Buf` from the input byte slice.
        let loaded_buf = Buf::new(bytes);
        // Create a `Ref` to the `Product` at the beginning of the buffer (offset 0).
        // This assumes the `Product` struct is at the start of the serialized data.
        let loaded_product_ref = Ref::<Product, endian::Little>::new(0 as usize);
        // Load the `Product` reference from the buffer. `unwrap()` is used here for simplicity,
        // but in a real application, error handling would be necessary.
        loaded_buf.load(loaded_product_ref).unwrap()
    }
}

/// Main function to demonstrate GrausDB usage with zero-copy serialization/deserialization structs.
/// It opens a database, sets a product, retrieves it, decreases its stock, and retrieves it again.
fn main() -> Result<(), Box<dyn Error>> {
    let db_path = "./grausdb_data";
    let db = GrausDb::open(db_path)?;

    println!("GrausDB opened at ='{:?}'", db_path);

    // Create a Product and serialize it into an OwnedBuf using zero-copy.
    let product_buf = Product::to_bytes(42, "Yeezy Boost 350 V2");

    // Define a key for the product and store it in the database.
    let key = b"yeezy".to_vec();
    db.set(key.clone(), &product_buf[..])?;

    // Retrieve the product bytes from the database.
    let loaded_bytes = db.get(&key)?.expect("Value not found");
    // Deserialize the bytes back into a `Product` reference using zero-copy.
    let loaded_product = Product::from_bytes(&loaded_bytes);

    // To access the name, which is a `Ref<str>`, we still need the original buffer.
    // This is a limitation of `Ref<str>` and zero-copy deserialization:
    // the `loaded_product` itself contains a `Ref<str>`, which needs a `Buf` to resolve
    // the actual string slice from the underlying byte buffer.
    let loaded_buf_for_name = Buf::new(&loaded_bytes);

    println!(
        "Loaded Product: stock = {}, name = {}",
        loaded_product.stock,
        loaded_buf_for_name.load(loaded_product.name)? // Resolve the `Ref<str>` to `&str`.
    );

    println!("Decreasing stock...");
    decrease_stock(key.clone(), &db)?;

    // Retrieve the product bytes again after the stock has been decreased.
    let loaded_bytes_after_decrease = db.get(&key)?.expect("Value not found after decrease");
    let loaded_product_after_decrease = Product::from_bytes(&loaded_bytes_after_decrease);
    let loaded_buf_for_name_after_decrease = Buf::new(&loaded_bytes_after_decrease);

    println!(
        "Loaded Product after decrease: stock = {}, name = {}",
        loaded_product_after_decrease.stock,
        loaded_buf_for_name_after_decrease.load(loaded_product_after_decrease.name)?
    );

    Ok(())
}

/// Decreases the stock of a product identified by `key` in the `GrausDb`.
/// This function performs an in-place, zero-copy update for maximum performance.
/// It directly modifies the `stock` field within the stored byte buffer.
fn decrease_stock(key: Vec<u8>, db: &GrausDb) -> Result<(), Box<dyn Error>> {
    // The `update_fn` closure is executed by `db.update_if` with mutable access
    // to the raw byte vector (`&mut Vec<u8>`) representing the stored value.
    let update_fn = |value: &mut Vec<u8>| {
        // Ensure the buffer is large enough to contain at least the stock field (u16).
        // The `stock` field is at the beginning of the `Product` struct due to `#[repr(C)]`.
        if value.len() < mem::size_of::<u16>() {
            panic!("Buffer too small to contain stock for key: {:?}", key);
        }

        // Read the current stock value (u16) from the first two bytes of the buffer.
        let current_stock = u16::from_le_bytes([value[0], value[1]]);

        // Decrement the stock, using `saturating_sub(1)` to prevent underflow (stock won't go below 0).
        let new_stock = current_stock.saturating_sub(1);

        // Convert the new stock value back into its little-endian byte representation.
        let new_stock_bytes = new_stock.to_le_bytes();
        // Write the new stock bytes directly back into the first two bytes of the buffer.
        value[0] = new_stock_bytes[0];
        value[1] = new_stock_bytes[1];
    };

    // Call `db.update_if` to atomically update the value associated with the key.
    //
    //
    // Note: The `None as Option<fn(&[u8]) -> bool>` cast is required by Rust compiler
    // infer the generic type `P` for the `predicate` parameter when `None` is provided,
    // resolving type inference ambiguity.
    db.update_if(
        key.clone(),
        update_fn,
        None,
        None as Option<fn(&[u8]) -> bool>,
    )
    .map_err(|e| e.into())
}
