use graus_db::GrausDb;
use musli_zerocopy::{endian, Buf, OwnedBuf, Ref, ZeroCopy};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs;
use std::time::Instant;

// SEE THE MAIN.RS FIRST TO KNOW ALL THE DETAILS.
// The following code is not commendted as it assumes you have read main.rs first.

#[derive(ZeroCopy)]
#[repr(C)]
struct Product {
    product_id: u64,
    stock: u16,
    price: u32,
    weight: f32,
    is_available: bool,
    category: Ref<str>,
    manufacturer: Ref<str>,
    dimensions: Dimensions,
    rating: f32,
    name: Ref<str>,
    description: Ref<str>,
}

#[derive(ZeroCopy)]
#[repr(C)]
struct Dimensions {
    length: u32,
    width: u32,
    height: u32,
}

#[derive(Serialize, Deserialize)]
struct ProductJson<'a> {
    product_id: u64,
    stock: u16,
    price: u32,
    weight: f32,
    is_available: bool,
    category: &'a str,
    manufacturer: &'a str,
    dimensions: DimensionsJson,
    rating: f32,
    name: &'a str,
    description: &'a str,
}

#[derive(Serialize, Deserialize)]
struct DimensionsJson {
    length: u32,
    width: u32,
    height: u32,
}

impl Product {
    fn to_bytes(
        product_id: u64,
        stock: u16,
        price: u32,
        weight: f32,
        is_available: bool,
        category_str: &str,
        manufacturer_str: &str,
        dimensions: Dimensions,
        rating: f32,
        name_str: &str,
        description_str: &str,
    ) -> OwnedBuf {
        let mut buf = OwnedBuf::new().with_byte_order::<endian::Little>();
        let product_ref = buf.store_uninit::<Product>();

        let category = buf.store_unsized(category_str);
        let manufacturer = buf.store_unsized(manufacturer_str);
        let name = buf.store_unsized(name_str);
        let description = buf.store_unsized(description_str);

        buf.load_uninit_mut(product_ref).write(&Product {
            product_id,
            stock,
            price,
            weight,
            is_available,
            category,
            manufacturer,
            dimensions,
            rating,
            name,
            description,
        });
        buf
    }

    fn from_bytes<'a>(bytes: &'a [u8]) -> &'a Product {
        let loaded_buf = Buf::new(bytes);
        let loaded_product_ref = Ref::<Product, endian::Little>::new(0 as usize);
        loaded_buf.load(loaded_product_ref).unwrap()
    }

    fn to_bytes_json(
        product_id: u64,
        stock: u16,
        price: u32,
        weight: f32,
        is_available: bool,
        category: &str,
        manufacturer: &str,
        dimensions: DimensionsJson,
        rating: f32,
        name: &str,
        description: &str,
    ) -> Vec<u8> {
        let product = ProductJson {
            product_id,
            stock,
            price,
            weight,
            is_available,
            category,
            manufacturer,
            dimensions,
            rating,
            name,
            description,
        };
        serde_json::to_vec(&product).unwrap()
    }

    fn from_bytes_json<'a>(bytes: &'a [u8]) -> ProductJson<'a> {
        serde_json::from_slice(bytes).unwrap()
    }
}

const ITERATIONS: usize = 1_000_000;

fn main() -> Result<(), Box<dyn Error>> {
    let db_path = "./grausdb_data";
    let _ = fs::remove_dir_all(db_path);
    let db = GrausDb::open(db_path)?;

    println!("GrausDB opened at ='{:?}'", db_path);

    let dimensions = Dimensions {
        length: 10,
        width: 5,
        height: 3,
    };

    let product_buf = Product::to_bytes(
        12345,
        15,
        10000,
        0.5,
        true,
        "Electronics",
        "Acme Corp",
        dimensions,
        4.5,
        "Yeezy Boost 350 V2",
        "Comfortable and stylish sneakers.",
    );
    let key = b"yeezy".to_vec();
    db.set(key.clone(), &product_buf[..])?;

    let start_time = Instant::now();

    for _i in 0..ITERATIONS {
        let loaded_bytes = db.get(&key)?.expect("Value not found");
        let loaded_product = Product::from_bytes(&loaded_bytes);
        assert_eq!(loaded_product.product_id, 12345);
        assert_eq!(loaded_product.stock, 15);
    }

    let duration = start_time.elapsed();
    println!("Zero-copy benchmark completed in {:?}", duration);

    // JSON benchmark
    println!("\nStarting JSON benchmark...");
    let dimensions_json = DimensionsJson {
        length: 10,
        width: 5,
        height: 3,
    };
    let key_json = b"yeezy_json".to_vec();
    let product_buf_json = Product::to_bytes_json(
        12345,
        15,
        10000,
        0.5,
        true,
        "Electronics",
        "Acme Corp",
        dimensions_json,
        4.5,
        "Yeezy Boost 350 V2",
        "Comfortable and stylish sneakers.",
    );
    db.set(key_json.clone(), &product_buf_json)?;

    let start_time_json = Instant::now();

    for _i in 0..ITERATIONS {
        let loaded_bytes = db.get(&key_json)?.expect("Value not found");
        let loaded_product = Product::from_bytes_json(&loaded_bytes);
        assert_eq!(loaded_product.product_id, 12345);
        assert_eq!(loaded_product.stock, 15);
    }

    let duration_json = start_time_json.elapsed();
    println!("JSON benchmark completed in {:?}", duration_json);

    Ok(())
}
