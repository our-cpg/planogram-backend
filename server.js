import express from 'express';
import cors from 'cors';
import pg from 'pg';
import crypto from 'crypto';

const app = express();
const PORT = process.env.PORT || 10000;
const { Pool } = pg;

// Database connection - SAME AS YOUR ORIGINAL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

// CORS middleware - SAME AS YOUR ORIGINAL
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Max-Age', '86400');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  next();
});

app.use(express.json());

// Track order processing status
let orderProcessingStatus = {
  isProcessing: false,
  lastCompleted: null,
  lastResult: null
};

// Initialize database tables with SAFE error handling
async function initDatabase() {
  console.log('🔄 Initializing database...');
  
  try {
    // Create products table
    console.log('📦 Creating products table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS products (
        variant_id BIGINT PRIMARY KEY,
        product_id BIGINT NOT NULL,
        title TEXT NOT NULL,
        variant_title TEXT,
        barcode TEXT,
        sku TEXT,
        price DECIMAL(10,2),
        compare_at_price DECIMAL(10,2),
        cost DECIMAL(10,2),
        inventory_quantity INTEGER,
        vendor TEXT,
        tags TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
      );
    `);
    console.log('✅ Products table ready');

    // Add vendor and tags columns if they don't exist
    try {
      await pool.query(`ALTER TABLE products ADD COLUMN IF NOT EXISTS vendor TEXT;`);
      await pool.query(`ALTER TABLE products ADD COLUMN IF NOT EXISTS tags TEXT;`);
      await pool.query(`ALTER TABLE products ADD COLUMN IF NOT EXISTS distributor TEXT;`);
      console.log('✅ Vendor, tags, and distributor columns added');
    } catch (err) {
      console.log('⚠️ Columns might already exist:', err.message);
    }

    // Create index
    console.log('📑 Creating barcode index...');
    try {
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_barcode ON products(barcode);`);
      console.log('✅ Index ready');
    } catch (err) {
      console.log('⚠️ Index already exists or error:', err.message);
    }

    // Create sales_data table with ALL columns
    console.log('📊 Creating sales_data table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS sales_data (
        variant_id BIGINT PRIMARY KEY,
        daily_sales INTEGER DEFAULT 0,
        weekly_sales INTEGER DEFAULT 0,
        monthly_sales INTEGER DEFAULT 0,
        quarterly_sales INTEGER DEFAULT 0,
        yearly_sales INTEGER DEFAULT 0,
        all_time_sales INTEGER DEFAULT 0,
        last_updated TIMESTAMP DEFAULT NOW()
      );
    `);
    console.log('✅ Sales_data table ready');

    // Try to add new columns (in case table exists from old version)
    console.log('🔧 Checking for new columns...');
    const columnsToAdd = [
      'daily_sales',
      'weekly_sales', 
      'quarterly_sales',
      'yearly_sales',
      'all_time_sales'
    ];

    for (const column of columnsToAdd) {
      try {
        await pool.query(`
          ALTER TABLE sales_data 
          ADD COLUMN IF NOT EXISTS ${column} INTEGER DEFAULT 0;
        `);
        console.log(`✅ Column ${column} ready`);
      } catch (err) {
        console.log(`⚠️ Column ${column} might already exist:`, err.message);
      }
    }

    console.log('✅✅✅ Database initialization complete!');
    
    // 🔥 NEW: Create orders table for full order tracking
    console.log('🛒 Creating orders table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS orders (
        order_id BIGINT PRIMARY KEY,
        order_number TEXT,
        customer_id BIGINT,
        customer_email_hash TEXT,
        total_price DECIMAL(10,2),
        subtotal_price DECIMAL(10,2),
        total_tax DECIMAL(10,2),
        order_date TIMESTAMP,
        is_returning_customer BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT NOW()
      );
    `);
    console.log('✅ Orders table ready');

    // Create order_items table for product sequences
    console.log('📦 Creating order_items table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS order_items (
        id SERIAL PRIMARY KEY,
        order_id BIGINT NOT NULL,
        variant_id BIGINT NOT NULL,
        product_id BIGINT NOT NULL,
        quantity INTEGER,
        price DECIMAL(10,2),
        cart_position INTEGER,
        created_at TIMESTAMP DEFAULT NOW(),
        FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
      );
    `);
    console.log('✅ Order_items table ready');

    // Create customer_stats table
    console.log('👥 Creating customer_stats table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_stats (
        customer_id BIGINT PRIMARY KEY,
        customer_email_hash TEXT,
        order_count INTEGER DEFAULT 0,
        total_spent DECIMAL(10,2) DEFAULT 0,
        average_order_value DECIMAL(10,2) DEFAULT 0,
        first_order_date TIMESTAMP,
        last_order_date TIMESTAMP,
        updated_at TIMESTAMP DEFAULT NOW()
      );
    `);
    console.log('✅ Customer_stats table ready');

    // Create product_correlations table for "bought together" analysis
    console.log('🤝 Creating product_correlations table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS product_correlations (
        id SERIAL PRIMARY KEY,
        product_a_id BIGINT NOT NULL,
        product_b_id BIGINT NOT NULL,
        co_purchase_count INTEGER DEFAULT 0,
        correlation_score DECIMAL(5,4),
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(product_a_id, product_b_id)
      );
    `);
    console.log('✅ Product_correlations table ready');

    // Create indexes for better query performance
    console.log('📑 Creating indexes...');
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_order_customer ON orders(customer_id);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_order_date ON orders(order_date);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_order_items_variant ON order_items(variant_id);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_correlation_product_a ON product_correlations(product_a_id);`);
    console.log('✅ All indexes ready');

    console.log('🔥🔥🔥 BEAST MODE DATABASE READY!');
  } catch (error) {
    console.error('❌ Database init error:', error);
    console.error('Stack:', error.stack);
  }
}

// Your original import products function
async function importProducts(storeName, accessToken) {
  console.log('🔄 Starting product import from Shopify...');
  console.log('📋 Filtering: Only products with proper title case (not ALL CAPS)');
  
  try {
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;
    let totalInserted = 0;
    let totalSkipped = 0;

    const isProperlyFormatted = (title) => {
      if (title === title.toUpperCase()) return false;
      if (!/[a-z]/.test(title)) return false;
      return true;
    };

    const cleanStoreName = (name) => {
      return name.replace('.myshopify.com', '');
    };

    const storeNameClean = cleanStoreName(storeName);

    while (hasNextPage) {
      const url = pageInfo 
        ? `https://${storeNameClean}.myshopify.com/admin/api/2025-10/products.json?limit=250&page_info=${pageInfo}`
        : `https://${storeNameClean}.myshopify.com/admin/api/2025-10/products.json?limit=250`;

      const response = await fetch(url, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error(`Shopify API error: ${response.status}`);
      }

      const data = await response.json();
      const products = data.products || [];
      
      pageCount++;
      console.log(`📦 Fetched page ${pageCount}: ${products.length} products`);

      for (const product of products) {
        if (!isProperlyFormatted(product.title)) {
          totalSkipped++;
          continue;
        }

        for (const variant of product.variants) {
          try {
            // Note: Distributor will be null on import, will be fetched when product is scanned
            await pool.query(`
              INSERT INTO products (
                variant_id, product_id, title, variant_title, barcode, sku, 
                price, compare_at_price, cost, inventory_quantity, 
                vendor, distributor, tags, created_at, updated_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
              ON CONFLICT (variant_id) 
              DO UPDATE SET
                title = EXCLUDED.title,
                variant_title = EXCLUDED.variant_title,
                barcode = EXCLUDED.barcode,
                sku = EXCLUDED.sku,
                price = EXCLUDED.price,
                compare_at_price = EXCLUDED.compare_at_price,
                cost = EXCLUDED.cost,
                inventory_quantity = EXCLUDED.inventory_quantity,
                vendor = EXCLUDED.vendor,
                tags = EXCLUDED.tags,
                updated_at = EXCLUDED.updated_at
            `, [
              variant.id,
              product.id,
              product.title,
              variant.title !== 'Default Title' ? variant.title : null,
              variant.barcode || null,
              variant.sku || null,
              parseFloat(variant.price) || 0,
              variant.compare_at_price ? parseFloat(variant.compare_at_price) : null,
              variant.inventory_management ? parseFloat(variant.price) * 0.6 : null,
              variant.inventory_quantity !== null && variant.inventory_quantity !== undefined ? variant.inventory_quantity : 0,
              product.vendor || null,
              null, // distributor - will be fetched on-demand
              Array.isArray(product.tags) ? product.tags.join(', ') : (product.tags || null),
              product.created_at,
              product.updated_at
            ]);
            
            totalInserted++;
          } catch (err) {
            console.error(`❌ Error inserting variant ${variant.id}:`, err.message);
          }
        }
      }

      console.log(`✅ Page ${pageCount}: ${totalInserted} inserted, ${totalSkipped} skipped`);

      const linkHeader = response.headers.get('Link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const nextMatch = linkHeader.match(/<[^>]*page_info=([^>&]+)>;\s*rel="next"/);
        pageInfo = nextMatch ? nextMatch[1] : null;
        hasNextPage = !!pageInfo;
      } else {
        hasNextPage = false;
      }

      await new Promise(resolve => setTimeout(resolve, 500));
    }

    console.log(`✅ Import complete! ${totalInserted} products inserted, ${totalSkipped} skipped`);
    
    // Now fetch inventory levels for all products
    console.log('📦 Fetching inventory levels...');
    await updateInventoryLevels(storeName, accessToken);
    
    return { 
      success: true, 
      message: `Successfully imported ${totalInserted} products (${totalSkipped} skipped)`,
      total: totalInserted,
      skipped: totalSkipped
    };

  } catch (error) {
    console.error('❌ Import failed:', error);
    throw error;
  }
}

// NEW: Fetch actual inventory levels from Shopify InventoryLevel API with improved rate limiting
async function updateInventoryLevels(storeName, accessToken) {
  console.log('📦 Updating inventory levels from Shopify...');
  
  try {
    const storeNameClean = storeName.replace('.myshopify.com', '');
    
    // Get all products from database
    const productsResult = await pool.query('SELECT variant_id FROM products');
    const variantIds = productsResult.rows.map(r => r.variant_id);
    
    console.log(`📊 Found ${variantIds.length} variants to update`);
    
    if (variantIds.length === 0) {
      console.log('⚠️ No variants found in database');
      return { success: true, updated: 0 };
    }
    
    // Process in batches of 50 using GraphQL
    const batchSize = 50;
    let updatedCount = 0;
    let errorCount = 0;
    const totalBatches = Math.ceil(variantIds.length / batchSize);
    
    // Helper function to process a single batch with retry logic
    const processBatch = async (batch, batchNumber, retryCount = 0) => {
      const maxRetries = 3;
      const baseDelay = 2000; // Start with 2 seconds
      
      try {
        console.log(`⏳ Processing batch ${batchNumber}/${totalBatches} (${batch.length} variants)${retryCount > 0 ? ` [Retry ${retryCount}]` : ''}`);
        
        // GraphQL query to get inventory for all variants in batch at once
        const graphqlQuery = `
          query getInventoryLevels($ids: [ID!]!) {
            nodes(ids: $ids) {
              ... on ProductVariant {
                id
                inventoryQuantity
                inventoryItem {
                  id
                  tracked
                }
              }
            }
          }
        `;
        
        // Convert variant IDs to GraphQL format
        const gqlVariantIds = batch.map(id => `gid://shopify/ProductVariant/${id}`);
        
        // Make single GraphQL request for entire batch
        const graphqlUrl = `https://${storeNameClean}.myshopify.com/admin/api/2024-01/graphql.json`;
        const response = await fetch(graphqlUrl, {
          method: 'POST',
          headers: {
            'X-Shopify-Access-Token': accessToken,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            query: graphqlQuery,
            variables: { ids: gqlVariantIds }
          })
        });
        
        // Check Shopify rate limit headers
        const rateLimitCost = response.headers.get('X-GraphQL-Cost-Include-Fields');
        const rateLimitRemaining = response.headers.get('X-Shopify-Shop-Api-Call-Limit');
        
        if (rateLimitRemaining) {
          const [used, total] = rateLimitRemaining.split('/').map(Number);
          const percentUsed = (used / total) * 100;
          
          // If we're using >80% of rate limit, slow down
          if (percentUsed > 80) {
            console.log(`⚠️ Rate limit at ${percentUsed.toFixed(1)}% (${used}/${total}), adding extra delay...`);
            await new Promise(resolve => setTimeout(resolve, 1000));
          }
        }
        
        // Handle rate limit response
        if (response.status === 429) {
          const retryAfter = response.headers.get('Retry-After');
          const waitTime = retryAfter ? parseInt(retryAfter) * 1000 : baseDelay * Math.pow(2, retryCount);
          
          console.log(`⏸️ Rate limit hit (429). Waiting ${waitTime / 1000}s before retry...`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
          
          // Retry this batch
          if (retryCount < maxRetries) {
            return await processBatch(batch, batchNumber, retryCount + 1);
          } else {
            console.error(`❌ Max retries (${maxRetries}) reached for batch ${batchNumber}`);
            return { success: false, updatedInBatch: 0, errorCount: batch.length };
          }
        }
        
        if (!response.ok) {
          const errorText = await response.text();
          console.error(`❌ GraphQL request failed (${response.status}):`, errorText);
          
          // For non-429 errors, maybe retry once
          if (retryCount < 1) {
            console.log(`🔄 Retrying batch ${batchNumber} after non-429 error...`);
            await new Promise(resolve => setTimeout(resolve, 2000));
            return await processBatch(batch, batchNumber, retryCount + 1);
          }
          
          return { success: false, updatedInBatch: 0, errorCount: batch.length };
        }
        
        const result = await response.json();
        
        // Check for GraphQL errors
        if (result.errors) {
          console.error('❌ GraphQL errors:', result.errors);
          
          // Check if it's a throttling error
          const isThrottled = result.errors.some(err => 
            err.message && (err.message.includes('throttled') || err.message.includes('rate limit'))
          );
          
          if (isThrottled && retryCount < maxRetries) {
            const waitTime = baseDelay * Math.pow(2, retryCount);
            console.log(`⏸️ Throttling detected in GraphQL response. Waiting ${waitTime / 1000}s...`);
            await new Promise(resolve => setTimeout(resolve, waitTime));
            return await processBatch(batch, batchNumber, retryCount + 1);
          }
          
          return { success: false, updatedInBatch: 0, errorCount: batch.length };
        }
        
        // Update database for all variants in batch
        const nodes = result.data?.nodes || [];
        let updatedInBatch = 0;
        let errorsInBatch = 0;
        
        for (let j = 0; j < nodes.length; j++) {
          const node = nodes[j];
          if (!node) {
            console.warn(`⚠️ Null node at index ${j} in batch ${batchNumber}`);
            errorsInBatch++;
            continue;
          }
          
          const variantId = batch[j];
          // Handle negative inventory properly - don't use || 0 which treats negatives as falsy
          const inventoryQuantity = node.inventoryQuantity !== null && node.inventoryQuantity !== undefined 
            ? node.inventoryQuantity 
            : 0;
          
          try {
            // Update database
            await pool.query(
              'UPDATE products SET inventory_quantity = $1, updated_at = NOW() WHERE variant_id = $2',
              [inventoryQuantity, variantId]
            );
            updatedInBatch++;
          } catch (dbError) {
            console.error(`❌ Database error for variant ${variantId}:`, dbError.message);
            errorsInBatch++;
          }
        }
        
        console.log(`✅ Batch ${batchNumber}/${totalBatches} complete (${updatedInBatch} updated in batch)`);
        
        return { success: true, updatedInBatch, errorCount: errorsInBatch };
        
      } catch (error) {
        console.error(`❌ Error processing batch ${batchNumber}:`, error.message);
        
        // Handle rate limits with exponential backoff
        if (error.message.includes('429') || error.message.includes('rate limit') || error.message.includes('throttled')) {
          if (retryCount < maxRetries) {
            const waitTime = baseDelay * Math.pow(2, retryCount);
            console.log(`⏸️ Rate limit detected. Waiting ${waitTime / 1000}s before retry ${retryCount + 1}/${maxRetries}...`);
            await new Promise(resolve => setTimeout(resolve, waitTime));
            return await processBatch(batch, batchNumber, retryCount + 1);
          }
        }
        
        return { success: false, updatedInBatch: 0, errorCount: batch.length };
      }
    };
    
    // Process all batches
    for (let i = 0; i < variantIds.length; i += batchSize) {
      const batch = variantIds.slice(i, i + batchSize);
      const batchNumber = Math.floor(i / batchSize) + 1;
      
      const result = await processBatch(batch, batchNumber);
      updatedCount += result.updatedInBatch;
      errorCount += result.errorCount;
      
      // Dynamic delay between batches based on progress
      if (i + batchSize < variantIds.length) {
        // Standard delay: 500ms (allows ~2 req/sec for GraphQL)
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }
    
    console.log(`✅ Inventory update complete!`);
    console.log(`   ✅ Successfully updated: ${updatedCount} variants`);
    console.log(`   ❌ Errors: ${errorCount} variants`);
    console.log(`   📊 Success rate: ${((updatedCount / variantIds.length) * 100).toFixed(1)}%`);
    
    return { 
      success: true, 
      updated: updatedCount,
      errors: errorCount,
      total: variantIds.length
    };
    
  } catch (error) {
    console.error('❌ Inventory update failed:', error);
    throw error;
  }
}

// UPDATED: Import sales data for multiple time periods
async function importSalesData(storeName, accessToken) {
  console.log('📊 Starting sales data import from Shopify Orders...');
  
  try {
    const now = new Date();
    const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    const oneMonthAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    const threeMonthsAgo = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);
    const oneYearAgo = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);
    
    const storeNameClean = storeName.replace('.myshopify.com', '');

    let allOrders = [];
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;

    console.log('📥 Fetching orders from last year...');

    while (hasNextPage) {
      const url = pageInfo 
        ? `https://${storeNameClean}.myshopify.com/admin/api/2025-10/orders.json?limit=250&page_info=${pageInfo}`
        : `https://${storeNameClean}.myshopify.com/admin/api/2025-10/orders.json?limit=250&status=any&created_at_min=${oneYearAgo.toISOString()}`;

      const response = await fetch(url, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        const errorBody = await response.text();
        console.error(`❌ Orders API failed with ${response.status} on page ${pageCount}`);
        console.error(`Response: ${errorBody}`);
        
        // If we already got some orders, break the loop and process what we have
        if (allOrders.length > 0) {
          console.log(`⚠️ Pagination failed, but we have ${allOrders.length} orders. Processing what we got...`);
          hasNextPage = false;
          break;
        }
        
        throw new Error(`Shopify Orders API error: ${response.status} - ${errorBody}`);
      }

      const data = await response.json();
      allOrders = allOrders.concat(data.orders || []);
      pageCount++;
      console.log(`📦 Fetched page ${pageCount}: ${data.orders?.length || 0} orders`);

      const linkHeader = response.headers.get('Link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const nextMatch = linkHeader.match(/<[^>]*page_info=([^>&]+)>;\s*rel="next"/);
        pageInfo = nextMatch ? nextMatch[1] : null;
        hasNextPage = !!pageInfo;
      } else {
        hasNextPage = false;
      }

      await new Promise(resolve => setTimeout(resolve, 500));
    }

    console.log(`✅ Fetched ${allOrders.length} total orders from last year`);

    // Calculate sales for each time period
    const salesByVariant = {};
    
    allOrders.forEach(order => {
      const orderDate = new Date(order.created_at);
      
      order.line_items?.forEach(item => {
        const variantId = item.variant_id;
        if (!variantId) return;

        if (!salesByVariant[variantId]) {
          salesByVariant[variantId] = {
            daily: 0,
            weekly: 0,
            monthly: 0,
            quarterly: 0,
            yearly: 0,
            allTime: 0
          };
        }

        const qty = item.quantity;
        salesByVariant[variantId].allTime += qty;
        
        if (orderDate >= oneDayAgo) salesByVariant[variantId].daily += qty;
        if (orderDate >= oneWeekAgo) salesByVariant[variantId].weekly += qty;
        if (orderDate >= oneMonthAgo) salesByVariant[variantId].monthly += qty;
        if (orderDate >= threeMonthsAgo) salesByVariant[variantId].quarterly += qty;
        if (orderDate >= oneYearAgo) salesByVariant[variantId].yearly += qty;
      });
    });

    console.log(`📊 Processing sales data for ${Object.keys(salesByVariant).length} variants...`);

    let updated = 0;
    for (const [variantId, sales] of Object.entries(salesByVariant)) {
      try {
        await pool.query(`
          INSERT INTO sales_data (
            variant_id, 
            daily_sales, 
            weekly_sales, 
            monthly_sales, 
            quarterly_sales, 
            yearly_sales, 
            all_time_sales,
            last_updated
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
          ON CONFLICT (variant_id) 
          DO UPDATE SET 
            daily_sales = $2,
            weekly_sales = $3,
            monthly_sales = $4,
            quarterly_sales = $5,
            yearly_sales = $6,
            all_time_sales = $7,
            last_updated = NOW()
        `, [
          variantId, 
          sales.daily, 
          sales.weekly, 
          sales.monthly, 
          sales.quarterly, 
          sales.yearly, 
          sales.allTime
        ]);
        updated++;
      } catch (err) {
        console.error(`❌ Error updating sales for variant ${variantId}:`, err.message);
      }
    }

    console.log(`✅ Sales data imported for ${updated} variants`);
    
    return { 
      success: true, 
      message: 'Sales data imported for all time periods',
      variantsUpdated: updated
    };

  } catch (error) {
    console.error('❌ Sales import failed:', error);
    console.error('Stack:', error.stack);
    throw error;
  }
}

// 🔥 BEAST MODE: Import full order data for customer analytics
async function importOrderData(storeName, accessToken) {
  console.log('🛒🔥 BEAST MODE: Starting order data import...');
  
  try {
    let allOrders = [];
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;
    const MAX_PAGES = 50; // Limit to prevent infinite loops (50 pages * 250 = 12,500 orders)

   // Fetch orders from Shopify - get last 6 months only
const sixMonthsAgo = new Date();
sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);
console.log(`📅 Fetching orders from last 6 months (since ${sixMonthsAgo.toISOString().split('T')[0]})...`);

while (hasNextPage && pageCount < MAX_PAGES) {
  let url;
  if (pageInfo) {
    // When paginating, ONLY use page_info (no other params)
    url = `https://${storeName}/admin/api/2024-10/orders.json?page_info=${pageInfo}`;
  } else {
    // First request: get orders from last 6 months
    url = `https://${storeName}/admin/api/2024-10/orders.json?limit=250&created_at_min=${sixMonthsAgo.toISOString()}`;
  }

      const response = await fetch(url, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`❌ Orders API failed: ${response.status}`, errorText);
        throw new Error(`Orders API error: ${response.status} - ${errorText}`);
      }

      const data = await response.json();
      allOrders = allOrders.concat(data.orders || []);
      pageCount++;
      console.log(`📦 Fetched page ${pageCount}: ${data.orders?.length || 0} orders (Total so far: ${allOrders.length})`);

      const linkHeader = response.headers.get('Link');
      console.log(`🔗 Link header:`, linkHeader);
      
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const nextMatch = linkHeader.match(/<[^>]*page_info=([^>&]+)>;\s*rel="next"/);
        if (nextMatch) {
          pageInfo = nextMatch[1];
          console.log(`➡️ Found next page with pageInfo: ${pageInfo.substring(0, 20)}...`);
        } else {
          console.log(`⚠️ Has 'next' in link header but couldn't parse page_info`);
          hasNextPage = false;
        }
      } else {
        console.log(`✅ No more pages - this was the last page`);
        hasNextPage = false;
      }
    }

    console.log(`🎯 Fetched ${allOrders.length} total orders`);

    // Handle stores with no orders
    if (allOrders.length === 0) {
      console.log('ℹ️ No orders found in store');
      return {
        success: true,
        ordersProcessed: 0,
        itemsProcessed: 0,
        ordersWithoutCustomers: 0,
        message: 'No orders found. Add some orders to your Shopify store to enable BEAST MODE analytics!',
        analytics: null // No data to analyze
      };
    }

    // Process each order
    let ordersProcessed = 0;
    let itemsProcessed = 0;
    let ordersWithoutCustomers = 0;
    const customerOrderCounts = new Map();

    for (const order of allOrders) {
      try {
        const customerId = order.customer?.id || null;
        const emailHash = order.customer?.email ? 
          crypto.createHash('md5').update(order.customer.email).digest('hex') : null;

        // Track orders without customers
        if (!customerId) {
          ordersWithoutCustomers++;
        }

        // Track customer order count (only if customer exists)
        if (customerId) {
          customerOrderCounts.set(customerId, (customerOrderCounts.get(customerId) || 0) + 1);
        }

        const isReturning = customerId ? (customerOrderCounts.get(customerId) > 1) : false;

        // Insert/Update order
        await pool.query(`
          INSERT INTO orders (
            order_id, order_number, customer_id, customer_email_hash,
            total_price, subtotal_price, total_tax, order_date, is_returning_customer
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          ON CONFLICT (order_id) DO UPDATE SET
            total_price = EXCLUDED.total_price,
            is_returning_customer = EXCLUDED.is_returning_customer
        `, [
          order.id,
          order.order_number || order.name,
          customerId,
          emailHash,
          parseFloat(order.total_price) || 0,
          parseFloat(order.subtotal_price) || 0,
          parseFloat(order.total_tax) || 0,
          order.created_at,
          isReturning
        ]);

        ordersProcessed++;

        // Insert order items (skip items without variant_id like custom items, gift cards)
        if (order.line_items && order.line_items.length > 0) {
          for (let i = 0; i < order.line_items.length; i++) {
            const item = order.line_items[i];
            
            // Skip items without variant_id (custom line items, gift cards, etc)
            if (!item.variant_id || !item.product_id) {
              console.log(`⚠️ Skipping line item without variant_id in order ${order.id}`);
              continue;
            }
            
            await pool.query(`
              INSERT INTO order_items (
                order_id, variant_id, product_id, quantity, price, cart_position
              ) VALUES ($1, $2, $3, $4, $5, $6)
              ON CONFLICT DO NOTHING
            `, [
              order.id,
              item.variant_id,
              item.product_id,
              item.quantity || 1,
              parseFloat(item.price) || 0,
              i + 1 // Cart position (1st product, 2nd product, etc.)
            ]);

            itemsProcessed++;
          }
        }

        // Update customer stats
        if (customerId) {
          await pool.query(`
            INSERT INTO customer_stats (
              customer_id, customer_email_hash, order_count, total_spent, 
              first_order_date, last_order_date
            ) 
            SELECT 
              $1, $2, 
              COUNT(*), 
              SUM(total_price),
              MIN(order_date),
              MAX(order_date)
            FROM orders WHERE customer_id = $1
            ON CONFLICT (customer_id) DO UPDATE SET
              order_count = EXCLUDED.order_count,
              total_spent = EXCLUDED.total_spent,
              average_order_value = EXCLUDED.total_spent / EXCLUDED.order_count,
              last_order_date = EXCLUDED.last_order_date,
              updated_at = NOW()
          `, [customerId, emailHash]);
        }

      } catch (err) {
        console.error(`❌ Error processing order ${order.id}:`, err.message);
      }
    }

    // Calculate product correlations (which products are bought together)
    console.log('🤝 Calculating product correlations...');
    await pool.query(`
      INSERT INTO product_correlations (product_a_id, product_b_id, co_purchase_count, correlation_score)
      SELECT 
        a.product_id as product_a_id,
        b.product_id as product_b_id,
        COUNT(DISTINCT a.order_id) as co_purchase_count,
        (COUNT(DISTINCT a.order_id)::DECIMAL / 
          (SELECT COUNT(DISTINCT order_id) FROM order_items WHERE product_id = a.product_id)) as correlation_score
      FROM order_items a
      JOIN order_items b ON a.order_id = b.order_id AND a.product_id < b.product_id
      GROUP BY a.product_id, b.product_id
      HAVING COUNT(DISTINCT a.order_id) >= 2
      ON CONFLICT (product_a_id, product_b_id) DO UPDATE SET
        co_purchase_count = EXCLUDED.co_purchase_count,
        correlation_score = EXCLUDED.correlation_score,
        updated_at = NOW()
    `);

    console.log(`✅🔥 BEAST MODE COMPLETE: ${ordersProcessed} orders, ${itemsProcessed} items processed!`);
    console.log(`📊 Orders without customers (guest checkouts): ${ordersWithoutCustomers}`);

    // 📊 Calculate time-based analytics
    console.log('📊 Calculating time-based analytics...');
    const hourCounts = new Array(24).fill(0);
    const dayCounts = { "1-5": 0, "6-10": 0, "11-15": 0, "16-20": 0, "21-25": 0, "26-31": 0 };

    allOrders.forEach(order => {
      const date = new Date(order.created_at);
      const hour = date.getHours();
      const day = date.getDate();
      
      // Count by hour (0-23)
      hourCounts[hour]++;
      
      // Count by day range
      if (day <= 5) dayCounts["1-5"]++;
      else if (day <= 10) dayCounts["6-10"]++;
      else if (day <= 15) dayCounts["11-15"]++;
      else if (day <= 20) dayCounts["16-20"]++;
      else if (day <= 25) dayCounts["21-25"]++;
      else dayCounts["26-31"]++;
    });

    // Format hour labels
    const hourLabels = [
      "12am-1am", "1am-2am", "2am-3am", "3am-4am", "4am-5am", "5am-6am",
      "6am-7am", "7am-8am", "8am-9am", "9am-10am", "10am-11am", "11am-12pm",
      "12pm-1pm", "1pm-2pm", "2pm-3pm", "3pm-4pm", "4pm-5pm", "5pm-6pm",
      "6pm-7pm", "7pm-8pm", "8pm-9pm", "9pm-10pm", "10pm-11pm", "11pm-12am"
    ];

    const byHour = hourCounts.map((count, hour) => ({
      hour,
      label: hourLabels[hour],
      count
    }));

    // Format day ranges
    const byDayOfMonth = Object.entries(dayCounts).map(([range, count]) => ({ 
      range, 
      count 
    }));

    // Calculate peaks
    const maxHourly = Math.max(...hourCounts);
    const maxDaily = Math.max(...Object.values(dayCounts));

    const peakHourIndex = hourCounts.indexOf(maxHourly);
    const peakHour = hourLabels[peakHourIndex];

    const peakDayRange = Object.entries(dayCounts)
      .reduce((a, b) => a[1] > b[1] ? a : b)[0];

    console.log(`📊 Analytics calculated: Peak hour = ${peakHour}, Peak days = ${peakDayRange}`);

    return {
      success: true,
      ordersProcessed,
      itemsProcessed,
      ordersWithoutCustomers,
      message: `Beast mode engaged! ${ordersProcessed} orders analyzed (${ordersWithoutCustomers} guest orders).`,
      analytics: {
        byHour,
        byDayOfMonth,
        maxHourly,
        maxDaily,
        peakHour,
        peakDayRange
      }
    };

  } catch (error) {
    console.error('❌ Order import failed:', error);
    throw error;
  }
}

// API Endpoints

app.get('/', (req, res) => {
  res.json({ 
    status: 'Planogram Backend Running - SALES TRACKING ENABLED', 
    timestamp: new Date().toISOString(),
    version: '2.0-sales-tracking'
  });
});

app.post('/api/shopify', async (req, res) => {
  const { storeName, accessToken, action } = req.body;

  console.log(`📥 API Request: ${action}`);

  if (!storeName || !accessToken) {
    return res.status(400).json({ error: 'Store name and access token required' });
  }

  try {
    if (action === 'connect') {
      const storeNameClean = storeName.replace('.myshopify.com', '');
      const testUrl = `https://${storeNameClean}.myshopify.com/admin/api/2025-10/shop.json`;
      
      console.log(`🔌 Testing connection to ${storeNameClean}...`);
      
      const response = await fetch(testUrl, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        console.error(`❌ Connection failed: ${response.status}`);
        return res.status(401).json({ error: 'Invalid Shopify credentials' });
      }

      const data = await response.json();
      console.log(`✅ Connected to shop: ${data.shop.name}`);
      
      res.json({ 
        success: true, 
        shop: data.shop.name,
        message: 'Connected to Shopify successfully'
      });

    } else if (action === 'refreshCache') {
      console.log('🔄 FULL REFRESH REQUESTED - Products + Sales...');
      
      console.log('Step 1/2: Importing products...');
      await importProducts(storeName, accessToken);
      
      console.log('Step 2/2: Importing sales data...');
      await importSalesData(storeName, accessToken);
      
      const countResult = await pool.query('SELECT COUNT(*) as count FROM products');
      const productCount = parseInt(countResult.rows[0].count);
      
      console.log(`✅✅✅ REFRESH COMPLETE! ${productCount} products with sales data ready`);
      
      res.json({ 
        success: true, 
        message: `Full refresh complete! ${productCount} products with sales data`,
        productCount
      });

    } else if (action === 'refreshProducts') {
      console.log('📦 PRODUCTS REFRESH REQUESTED...');
      
      await importProducts(storeName, accessToken);
      
      const countResult = await pool.query('SELECT COUNT(*) as count FROM products');
      const productCount = parseInt(countResult.rows[0].count);
      
      console.log(`✅ Products imported: ${productCount}`);
      
      res.json({ 
        success: true, 
        message: `${productCount} products imported`,
        productCount
      });

    } else if (action === 'refreshSales') {
      console.log('📊 SALES REFRESH REQUESTED...');
      
      await importSalesData(storeName, accessToken);
      
      const salesResult = await pool.query('SELECT COUNT(*) as count FROM sales_data WHERE monthly_sales > 0');
      const salesCount = parseInt(salesResult.rows[0].count);
      
      console.log(`✅ Sales data updated: ${salesCount} variants with sales`);
      
      res.json({ 
        success: true, 
        message: `Sales updated! ${salesCount} products with sales data`,
        salesCount
      });

    } else if (action === 'refreshInventory') {
      console.log('📦 INVENTORY REFRESH REQUESTED...');
      
      // Run in background to avoid timeout
      updateInventoryLevels(storeName, accessToken)
        .then(result => {
          console.log(`✅ Inventory update complete: ${result.updated} variants`);
        })
        .catch(error => {
          console.error('❌ Inventory update failed:', error);
        });
      
      // Return immediately
      res.json({ 
        success: true, 
        message: 'Inventory refresh started in background. This will take 15-20 minutes for 934 products.',
        processing: true
      });

   } else if (action === 'refreshOrders') {
      console.log('🔥🛒 BEAST MODE ORDER REFRESH REQUESTED...');
      
      if (orderProcessingStatus.isProcessing) {
        return res.json({
          success: false,
          message: 'Order processing already in progress. Please wait.',
          processing: true
        });
      }
      
      orderProcessingStatus.isProcessing = true;
      
      // Start processing in background (don't await)
      importOrderData(storeName, accessToken)
        .then(result => {
          console.log(`✅ Background processing complete: ${result.ordersProcessed} orders`);
          orderProcessingStatus.isProcessing = false;
          orderProcessingStatus.lastCompleted = new Date();
          orderProcessingStatus.lastResult = result;
        })
        .catch(error => {
          console.error('❌ Background processing failed:', error);
          orderProcessingStatus.isProcessing = false;
        });
      
      // Return immediately
      res.json({ 
        success: true, 
        message: 'Order processing started in background. Check /api/orders/status for progress.',
        processing: true
      });

    } else {
      res.status(400).json({ error: 'Invalid action' });
    }

  } catch (error) {
    console.error('❌ API Error:', error);
    console.error('Stack:', error.stack);
    res.status(500).json({ error: error.message, stack: error.stack });
  }
});

// UPDATED: Return sales data for all time periods
app.get('/api/product/:upc', async (req, res) => {
  const { upc } = req.params;
  
  console.log(`🔍 Searching for UPC: "${upc}"`);

  try {
    const result = await pool.query(`
      SELECT 
        p.*,
        COALESCE(s.daily_sales, 0) as daily_sales,
        COALESCE(s.weekly_sales, 0) as weekly_sales,
        COALESCE(s.monthly_sales, 0) as monthly_sales,
        COALESCE(s.quarterly_sales, 0) as quarterly_sales,
        COALESCE(s.yearly_sales, 0) as yearly_sales,
        COALESCE(s.all_time_sales, 0) as all_time_sales
      FROM products p
      LEFT JOIN sales_data s ON p.variant_id = s.variant_id
      WHERE p.barcode = $1
      LIMIT 1
    `, [upc]);

    if (result.rows.length === 0) {
      console.log(`❌ No product found with barcode: "${upc}"`);
      return res.status(404).json({ error: 'Product not found' });
    }

    const product = result.rows[0];
    
    // Fetch distributor from Shopify metafields if not already in database
    let distributor = product.distributor;
    if (!distributor && req.query.fetchMetafields !== 'false') {
      try {
        // Get Shopify credentials from request (they should be passed as query params or we use stored ones)
        const storeName = req.query.storeName || process.env.SHOPIFY_STORE_NAME;
        const accessToken = req.query.accessToken || process.env.SHOPIFY_ACCESS_TOKEN;
        
        if (storeName && accessToken && product.product_id) {
          const metafieldsUrl = `https://${storeName}/admin/api/2025-10/products/${product.product_id}/metafields.json`;
          console.log(`🔍 Fetching metafields from: ${metafieldsUrl}`);
          const metafieldsResponse = await fetch(metafieldsUrl, {
            headers: {
              'X-Shopify-Access-Token': accessToken,
              'Content-Type': 'application/json'
            }
          });
          
          if (metafieldsResponse.ok) {
            const metafieldsData = await metafieldsResponse.json();
            console.log(`📦 Metafields received:`, metafieldsData.metafields?.length || 0, 'metafields');
            console.log(`🔍 Looking for custom.vendor metafield...`);
            
            const distributorMetafield = metafieldsData.metafields?.find(
              mf => mf.namespace === 'custom' && mf.key === 'vendor'
            );
            
            if (distributorMetafield?.value) {
              distributor = distributorMetafield.value;
              // Update database with distributor
              await pool.query(
                'UPDATE products SET distributor = $1 WHERE variant_id = $2',
                [distributor, product.variant_id]
              );
              console.log(`✅ Fetched and saved distributor: ${distributor}`);
            } else {
              console.log(`⚠️ No custom.vendor metafield found`);
            }
          } else {
            console.log(`❌ Metafields fetch failed: ${metafieldsResponse.status}`);
          }
        }
      } catch (metafieldError) {
        console.log('⚠️ Could not fetch metafields:', metafieldError.message);
        // Don't fail the whole request if metafields fail
      }
    }
    
    // 🔥 BEAST MODE: Get analytics data
    const analyticsQuery = await pool.query(`
      SELECT 
        COUNT(DISTINCT oi.order_id) as times_purchased,
        AVG(o.total_price) as average_order_value,
        SUM(CASE WHEN o.is_returning_customer THEN 1 ELSE 0 END) as returning_customer_purchases,
        SUM(CASE WHEN NOT o.is_returning_customer THEN 1 ELSE 0 END) as new_customer_purchases,
        AVG(oi.cart_position) as average_cart_position
      FROM order_items oi
      JOIN orders o ON oi.order_id = o.order_id
      WHERE oi.variant_id = $1
    `, [product.variant_id]);
    
    const analytics = analyticsQuery.rows[0] || {};
    
    // Get top 3 products bought together
    const correlationsQuery = await pool.query(`
      SELECT 
        p.title,
        p.variant_title,
        pc.co_purchase_count,
        pc.correlation_score
      FROM product_correlations pc
      JOIN products p ON (p.product_id = pc.product_b_id OR p.product_id = pc.product_a_id)
      WHERE (pc.product_a_id = $1 OR pc.product_b_id = $1)
        AND p.product_id != $1
      ORDER BY pc.co_purchase_count DESC
      LIMIT 3
    `, [product.product_id]);
    
    const boughtTogether = correlationsQuery.rows.map(r => ({
      name: r.variant_title ? `${r.title} - ${r.variant_title}` : r.title,
      count: parseInt(r.co_purchase_count),
      score: parseFloat(r.correlation_score)
    }));

    console.log(`✅ Found: ${product.title}`);
    console.log(`📊 Sales: Day=${product.daily_sales}, Week=${product.weekly_sales}, Month=${product.monthly_sales}`);
    console.log(`📦 Stock: ${product.inventory_quantity} units`);
    console.log(`💰 AOV: $${analytics.average_order_value || 0}`);

    res.json({
      title: product.title,
      variantTitle: product.variant_title,
      price: parseFloat(product.price),
      compareAtPrice: product.compare_at_price ? parseFloat(product.compare_at_price) : null,
      cost: product.cost ? parseFloat(product.cost) : null, // Don't assume - return null if not available
      dailySales: parseInt(product.daily_sales) || 0,
      weeklySales: parseInt(product.weekly_sales) || 0,
      monthlySales: parseInt(product.monthly_sales) || 0,
      quarterlySales: parseInt(product.quarterly_sales) || 0,
      yearlySales: parseInt(product.yearly_sales) || 0,
      allTimeSales: parseInt(product.all_time_sales) || 0,
      barcode: product.barcode,
      sku: product.sku,
      inventoryQuantity: product.inventory_quantity !== null && product.inventory_quantity !== undefined 
        ? parseInt(product.inventory_quantity) 
        : 0,
      vendor: product.vendor,
      distributor: distributor,
      tags: product.tags,
      createdAt: product.created_at,
      updatedAt: product.updated_at,
      // 🔥 BEAST MODE Analytics
      analytics: {
        timesPurchased: parseInt(analytics.times_purchased) || 0,
        averageOrderValue: parseFloat(analytics.average_order_value) || 0,
        returningCustomerPurchases: parseInt(analytics.returning_customer_purchases) || 0,
        newCustomerPurchases: parseInt(analytics.new_customer_purchases) || 0,
        averageCartPosition: parseFloat(analytics.average_cart_position) || 0,
        boughtTogether: boughtTogether
      }
    });

  } catch (error) {
    console.error('❌ Database error:', error);
    console.error('Stack:', error.stack);
    res.status(500).json({ error: 'Database query failed', details: error.message });
  }
});

// NEW: Get LIVE inventory directly from Shopify (not database)
app.get('/api/shopify/live-inventory/:upc', async (req, res) => {
  const { upc } = req.params;
  const storeName = req.query.storeName || process.env.SHOPIFY_STORE_NAME;
  const accessToken = req.query.accessToken || process.env.SHOPIFY_ACCESS_TOKEN;
  
  if (!storeName || !accessToken) {
    return res.status(400).json({ error: 'Missing Shopify credentials' });
  }
  
  console.log(`🔴 LIVE FETCH from Shopify for UPC: ${upc}`);
  
  try {
    const storeNameClean = storeName.replace('.myshopify.com', '');
    
    // First, get the product by barcode to find variant ID
    const searchQuery = `
      query getProductByBarcode($barcode: String!) {
        productVariants(first: 1, query: $barcode) {
          edges {
            node {
              id
              title
              barcode
              inventoryQuantity
              price
              compareAtPrice
              sku
              product {
                id
                title
                vendor
                tags
                createdAt
              }
              inventoryItem {
                id
                tracked
              }
            }
          }
        }
      }
    `;
    
    const graphqlUrl = `https://${storeNameClean}.myshopify.com/admin/api/2024-01/graphql.json`;
    const response = await fetch(graphqlUrl, {
      method: 'POST',
      headers: {
        'X-Shopify-Access-Token': accessToken,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        query: searchQuery,
        variables: { barcode: `barcode:${upc}` }
      })
    });
    
    if (!response.ok) {
      console.error(`❌ Shopify API error: ${response.status}`);
      return res.status(response.status).json({ error: 'Shopify API error' });
    }
    
    const result = await response.json();
    
    if (result.errors) {
      console.error('❌ GraphQL errors:', result.errors);
      return res.status(500).json({ error: 'GraphQL query failed', details: result.errors });
    }
    
    const edges = result.data?.productVariants?.edges || [];
    
    if (edges.length === 0) {
      console.log(`❌ No product found with barcode: ${upc}`);
      return res.status(404).json({ error: 'Product not found in Shopify' });
    }
    
    const variant = edges[0].node;
    const product = variant.product;
    
    // Get the numeric variant ID from the GID
    const variantId = variant.id.split('/').pop();
    
    // Now get sales data from our database (if available)
    let salesData = {
      dailySales: 0,
      weeklySales: 0,
      monthlySales: 0,
      quarterlySales: 0,
      yearlySales: 0,
      allTimeSales: 0
    };
    
    try {
      const dbResult = await pool.query(`
        SELECT 
          COALESCE(s.daily_sales, 0) as daily_sales,
          COALESCE(s.weekly_sales, 0) as weekly_sales,
          COALESCE(s.monthly_sales, 0) as monthly_sales,
          COALESCE(s.quarterly_sales, 0) as quarterly_sales,
          COALESCE(s.yearly_sales, 0) as yearly_sales,
          COALESCE(s.all_time_sales, 0) as all_time_sales
        FROM sales_data s
        WHERE s.variant_id = $1
      `, [variantId]);
      
      if (dbResult.rows.length > 0) {
        const row = dbResult.rows[0];
        salesData = {
          dailySales: parseInt(row.daily_sales) || 0,
          weeklySales: parseInt(row.weekly_sales) || 0,
          monthlySales: parseInt(row.monthly_sales) || 0,
          quarterlySales: parseInt(row.quarterly_sales) || 0,
          yearlySales: parseInt(row.yearly_sales) || 0,
          allTimeSales: parseInt(row.all_time_sales) || 0
        };
      }
    } catch (dbError) {
      console.log('⚠️ Could not fetch sales data from database:', dbError.message);
    }
    
    console.log(`✅ LIVE from Shopify: ${product.title} - Stock: ${variant.inventoryQuantity}`);
    
    res.json({
      title: product.title,
      variantTitle: variant.title !== 'Default Title' ? variant.title : null,
      price: parseFloat(variant.price),
      compareAtPrice: variant.compareAtPrice ? parseFloat(variant.compareAtPrice) : null,
      barcode: variant.barcode,
      sku: variant.sku,
      inventoryQuantity: variant.inventoryQuantity, // LIVE from Shopify!
      vendor: product.vendor,
      tags: Array.isArray(product.tags) ? product.tags.join(', ') : product.tags,
      createdAt: product.createdAt,
      ...salesData,
      source: 'shopify_live'
    });
    
  } catch (error) {
    console.error('❌ Error fetching live inventory:', error);
    res.status(500).json({ error: 'Failed to fetch live inventory', details: error.message });
  }
});

// NEW: Get ALL products with LIVE inventory from Shopify (not database!)
app.get('/api/shopify/live-products-all', async (req, res) => {
  const storeName = req.query.storeName || process.env.SHOPIFY_STORE_NAME;
  const accessToken = req.query.accessToken || process.env.SHOPIFY_ACCESS_TOKEN;
  
  if (!storeName || !accessToken) {
    return res.status(400).json({ error: 'Missing Shopify credentials' });
  }
  
  console.log('🔴 FETCHING ALL PRODUCTS WITH LIVE INVENTORY FROM SHOPIFY');
  
  try {
    const storeNameClean = storeName.replace('.myshopify.com', '');
    
    let allProducts = [];
    let hasNextPage = true;
    let cursor = null;
    let pageCount = 0;
    
    while (hasNextPage) {
      pageCount++;
      console.log(`📦 Fetching page ${pageCount}...`);
      
      const query = `
        query getProductsWithInventory($cursor: String) {
          productVariants(first: 250, after: $cursor) {
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              node {
                id
                title
                barcode
                sku
                price
                inventoryQuantity
                product {
                  id
                  title
                  vendor
                  tags
                }
              }
            }
          }
        }
      `;
      
      const response = await fetch(`https://${storeNameClean}.myshopify.com/admin/api/2024-01/graphql.json`, {
        method: 'POST',
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          query: query,
          variables: cursor ? { cursor } : {}
        })
      });
      
      if (!response.ok) {
        throw new Error(`Shopify API error: ${response.status}`);
      }
      
      const result = await response.json();
      
      if (result.errors) {
        console.error('GraphQL errors:', result.errors);
        throw new Error('GraphQL query failed');
      }
      
      const edges = result.data?.productVariants?.edges || [];
      
      for (const edge of edges) {
        const variant = edge.node;
        const product = variant.product;
        
        // Get sales data AND distributor from database if available
        const variantId = variant.id.split('/').pop();
        const productId = product.id.split('/').pop();
        let salesData = {
          dailySales: 0,
          weeklySales: 0,
          monthlySales: 0,
          allTimeSales: 0
        };
        let distributor = null;
        
        try {
          const dbResult = await pool.query(`
            SELECT 
              COALESCE(s.daily_sales, 0) as daily_sales,
              COALESCE(s.weekly_sales, 0) as weekly_sales,
              COALESCE(s.monthly_sales, 0) as monthly_sales,
              COALESCE(s.all_time_sales, 0) as all_time_sales,
              p.distributor
            FROM sales_data s
            FULL OUTER JOIN products p ON s.variant_id = p.variant_id
            WHERE p.variant_id = $1 OR p.product_id = $2
            LIMIT 1
          `, [variantId, productId]);
          
          if (dbResult.rows.length > 0) {
            const row = dbResult.rows[0];
            salesData = {
              dailySales: parseInt(row.daily_sales) || 0,
              weeklySales: parseInt(row.weekly_sales) || 0,
              monthlySales: parseInt(row.monthly_sales) || 0,
              allTimeSales: parseInt(row.all_time_sales) || 0
            };
            distributor = row.distributor;
          }
        } catch (dbError) {
          // Continue without sales data/distributor if DB lookup fails
        }
        
        allProducts.push({
          variantId: variantId,
          title: product.title,
          variantTitle: variant.title !== 'Default Title' ? variant.title : null,
          barcode: variant.barcode,
          sku: variant.sku,
          price: parseFloat(variant.price) || 0,
          stock: variant.inventoryQuantity, // LIVE from Shopify!
          vendor: product.vendor,
          distributor: distributor, // From database (synced daily)
          tags: Array.isArray(product.tags) ? product.tags.join(', ') : product.tags,
          ...salesData,
          source: 'shopify_live'
        });
      }
      
      const pageInfo = result.data?.productVariants?.pageInfo;
      hasNextPage = pageInfo?.hasNextPage || false;
      cursor = pageInfo?.endCursor;
      
      // Rate limiting
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    console.log(`✅ Fetched ${allProducts.length} products with LIVE inventory from Shopify`);
    
    // Calculate summary
    const criticalCount = allProducts.filter(p => {
      const velocity = p.dailySales || 0;
      const daysLeft = velocity > 0 ? Math.floor(p.stock / velocity) : (p.stock > 0 ? 999 : 0);
      return daysLeft <= 3;
    }).length;
    
    res.json({
      products: allProducts,
      summary: {
        totalProducts: allProducts.length,
        criticalAlerts: criticalCount,
        source: 'shopify_live'
      }
    });
    
  } catch (error) {
    console.error('❌ Error fetching live products:', error);
    res.status(500).json({ error: error.message });
  }
});

// ============================================
// METAFIELD SYNC - Background job to update distributor data
// ============================================

async function syncMetafields(storeName, accessToken) {
  console.log('🔧 Starting metafield sync for all products...');
  
  try {
    const storeNameClean = storeName.replace('.myshopify.com', '');
    
    // Get all products from database
    const productsResult = await pool.query('SELECT DISTINCT product_id, title FROM products WHERE product_id IS NOT NULL');
    const products = productsResult.rows;
    
    console.log(`📦 Found ${products.length} unique products to sync metafields`);
    
    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    
    // Process in batches to avoid overwhelming the API
    const batchSize = 5; // Smaller batches to avoid rate limits
    
    for (let i = 0; i < products.length; i += batchSize) {
      const batch = products.slice(i, i + batchSize);
      const batchNumber = Math.floor(i / batchSize) + 1;
      const totalBatches = Math.ceil(products.length / batchSize);
      
      console.log(`⏳ Processing metafield batch ${batchNumber}/${totalBatches}...`);
      
      // Process batch sequentially (not in parallel) to avoid rate limits
      for (const product of batch) {
        let retryCount = 0;
        const maxRetries = 3;
        
        while (retryCount <= maxRetries) {
          try {
            const metafieldsUrl = `https://${storeNameClean}.myshopify.com/admin/api/2025-10/products/${product.product_id}/metafields.json`;
            
            const response = await fetch(metafieldsUrl, {
              headers: {
                'X-Shopify-Access-Token': accessToken,
                'Content-Type': 'application/json'
              }
            });
            
            if (response.status === 429) {
              // Rate limited - wait and retry
              const waitTime = 2000 * Math.pow(2, retryCount); // Exponential backoff
              console.log(`⏸️ Rate limit hit, waiting ${waitTime / 1000}s...`);
              await new Promise(resolve => setTimeout(resolve, waitTime));
              retryCount++;
              continue;
            }
            
            if (!response.ok) {
              if (response.status === 404) {
                // Product deleted from Shopify
                skippedCount++;
                break;
              }
              throw new Error(`API error: ${response.status}`);
            }
            
            const metafieldsData = await response.json();
            const distributorMetafield = metafieldsData.metafields?.find(
              mf => mf.namespace === 'custom' && mf.key === 'vendor'
            );
            
            if (distributorMetafield?.value) {
              // Update all variants of this product with the distributor
              await pool.query(
                'UPDATE products SET distributor = $1 WHERE product_id = $2',
                [distributorMetafield.value, product.product_id]
              );
              successCount++;
            } else {
              skippedCount++;
            }
            
            // Success - break retry loop
            break;
            
          } catch (error) {
            if (retryCount >= maxRetries) {
              console.error(`❌ Error fetching metafields for product ${product.product_id}:`, error.message);
              errorCount++;
              break;
            }
            retryCount++;
          }
        }
        
        // Small delay between each product to avoid rate limits
        await new Promise(resolve => setTimeout(resolve, 300)); // 300ms per product
      }
      
      // Longer delay between batches
      if (i + batchSize < products.length) {
        await new Promise(resolve => setTimeout(resolve, 2000)); // 2 seconds between batches
      }
    }
    
    console.log(`✅ Metafield sync complete!`);
    console.log(`   ✅ Updated: ${successCount} products`);
    console.log(`   ⏭️  Skipped: ${skippedCount} products (no metafield or deleted)`);
    console.log(`   ❌ Errors: ${errorCount} products`);
    
    return {
      success: true,
      updated: successCount,
      skipped: skippedCount,
      errors: errorCount,
      total: products.length
    };
    
  } catch (error) {
    console.error('❌ Metafield sync failed:', error);
    throw error;
  }
}

// Manual metafield sync endpoint
app.post('/api/sync-metafields', async (req, res) => {
  try {
    const storeName = req.body.storeName || process.env.SHOPIFY_STORE_NAME;
    const accessToken = req.body.accessToken || process.env.SHOPIFY_ACCESS_TOKEN;
    
    if (!storeName || !accessToken) {
      return res.status(400).json({ 
        success: false, 
        error: 'Missing storeName or accessToken' 
      });
    }
    
    console.log('🔧 Manual metafield sync triggered');
    const result = await syncMetafields(storeName, accessToken);
    
    res.json({
      success: true,
      message: 'Metafield sync completed',
      ...result
    });
  } catch (error) {
    console.error('❌ Metafield sync failed:', error);
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// Auto-sync metafields daily
const METAFIELD_SYNC_INTERVAL = 24 * 60 * 60 * 1000; // 24 hours

async function autoSyncMetafields() {
  const storeName = process.env.SHOPIFY_STORE_NAME || '1m3tfh-xt.myshopify.com';
  const accessToken = process.env.SHOPIFY_ACCESS_TOKEN;
  
  if (!accessToken) {
    console.log('⚠️ No access token configured for metafield auto-sync');
    return;
  }
  
  try {
    console.log('⏰ Running scheduled metafield sync...');
    const result = await syncMetafields(storeName, accessToken);
    console.log(`✅ Scheduled metafield sync complete: ${result.updated}/${result.total} updated`);
  } catch (error) {
    console.error('❌ Scheduled metafield sync failed:', error.message);
  }
}

// Run initial metafield sync 2 minutes after server starts (let inventory sync finish first)
setTimeout(() => {
  console.log('🚀 Running initial metafield sync...');
  autoSyncMetafields();
}, 2 * 60 * 1000);

// Then run every 24 hours
setInterval(autoSyncMetafields, METAFIELD_SYNC_INTERVAL);
console.log(`🔧 Metafield auto-sync enabled: will run every 24 hours`);

// Get all products with forecasting data
app.get('/api/products/all', async (req, res) => {
  try {
    console.log('📊 Fetching all products with forecasting data...');
    
    const result = await pool.query(`
      SELECT 
        p.variant_id,
        p.product_id,
        p.title,
        p.variant_title,
        p.barcode,
        p.sku,
        p.price,
        p.cost,
        p.inventory_quantity,
        p.vendor,
        p.tags,
        COALESCE(s.daily_sales, 0) as daily_sales,
        COALESCE(s.weekly_sales, 0) as weekly_sales,
        COALESCE(s.monthly_sales, 0) as monthly_sales,
        COALESCE(s.all_time_sales, 0) as all_time_sales
      FROM products p
      LEFT JOIN sales_data s ON p.variant_id = s.variant_id
      ORDER BY p.title, p.variant_title
    `);
    
    const products = result.rows.map(row => {
      const stock = row.inventory_quantity !== null && row.inventory_quantity !== undefined 
        ? parseInt(row.inventory_quantity) 
        : 0;
      const velocity = parseFloat(row.daily_sales) || 0;
      const daysLeft = velocity > 0 ? Math.floor(stock / velocity) : (stock > 0 ? 999 : 0);
      
      // Determine risk level
      let risk = 'LOW';
      if (daysLeft < 0) risk = 'CRITICAL';
      else if (daysLeft <= 3) risk = 'CRITICAL';
      else if (daysLeft <= 7) risk = 'HIGH';
      else if (daysLeft <= 14) risk = 'MEDIUM';
      
      return {
        variantId: row.variant_id,
        productId: row.product_id,
        title: row.title,
        variantTitle: row.variant_title,
        barcode: row.barcode,
        sku: row.sku,
        price: parseFloat(row.price) || 0,
        cost: parseFloat(row.cost) || 0,
        stock: stock,
        velocity: velocity,
        daysLeft: daysLeft,
        risk: risk,
        vendor: row.vendor,
        tags: row.tags,
        dailySales: parseInt(row.daily_sales) || 0,
        weeklySales: parseInt(row.weekly_sales) || 0,
        monthlySales: parseInt(row.monthly_sales) || 0,
        allTimeSales: parseInt(row.all_time_sales) || 0
      };
    });
    
    // Calculate summary stats
    const criticalCount = products.filter(p => p.daysLeft <= 3).length;
    const highRiskCount = products.filter(p => p.daysLeft > 3 && p.daysLeft <= 7).length;
    const totalProducts = products.length;
    const avgVelocity = products.reduce((sum, p) => sum + p.velocity, 0) / totalProducts;
    
    console.log(`✅ Returned ${totalProducts} products`);
    console.log(`📊 Critical: ${criticalCount}, High Risk: ${highRiskCount}`);
    
    res.json({
      products: products,
      summary: {
        totalProducts: totalProducts,
        criticalAlerts: criticalCount,
        highRisk: highRiskCount,
        avgVelocity: avgVelocity.toFixed(2)
      }
    });
    
  } catch (error) {
    console.error('❌ Error fetching products:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/stats', async (req, res) => {
  try {
    const productCount = await pool.query('SELECT COUNT(*) as count FROM products');
    const salesCount = await pool.query('SELECT COUNT(*) as count FROM sales_data WHERE monthly_sales > 0');
    const totalSales = await pool.query('SELECT SUM(monthly_sales) as total FROM sales_data');
    
    res.json({
      products: parseInt(productCount.rows[0].count),
      productsWithSales: parseInt(salesCount.rows[0].count),
      totalMonthlySales: parseInt(totalSales.rows[0].total) || 0
    });
  } catch (error) {
    console.error('❌ Stats error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get top product correlations (products bought together)
app.get('/api/correlations', async (req, res) => {
  try {
    console.log('🤝 Fetching top product correlations...');
    
    const result = await pool.query(`
      SELECT 
        pa.title as product_a_name,
        pa.variant_title as product_a_variant,
        pa.barcode as product_a_upc,
        pa.price as product_a_price,
        pb.title as product_b_name,
        pb.variant_title as product_b_variant,
        pb.barcode as product_b_upc,
        pb.price as product_b_price,
        pc.co_purchase_count,
        pc.correlation_score
      FROM product_correlations pc
      JOIN products pa ON pa.product_id = pc.product_a_id
      JOIN products pb ON pb.product_id = pc.product_b_id
      ORDER BY pc.co_purchase_count DESC, pc.correlation_score DESC
      LIMIT 20
    `);

    const correlations = result.rows.map(r => ({
      productA: {
        name: r.product_a_variant ? `${r.product_a_name} - ${r.product_a_variant}` : r.product_a_name,
        upc: r.product_a_upc,
        price: parseFloat(r.product_a_price)
      },
      productB: {
        name: r.product_b_variant ? `${r.product_b_name} - ${r.product_b_variant}` : r.product_b_name,
        upc: r.product_b_upc,
        price: parseFloat(r.product_b_price)
      },
      timesBoughtTogether: parseInt(r.co_purchase_count),
      correlationScore: parseFloat(r.correlation_score)
    }));

    res.json({ correlations });
  } catch (error) {
    console.error('❌ Correlations error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get order processing status
app.get('/api/orders/status', (req, res) => {
  res.json({
    isProcessing: orderProcessingStatus.isProcessing,
    lastCompleted: orderProcessingStatus.lastCompleted,
    lastResult: orderProcessingStatus.lastResult ? {
      ordersProcessed: orderProcessingStatus.lastResult.ordersProcessed,
      itemsProcessed: orderProcessingStatus.lastResult.itemsProcessed,
      analytics: orderProcessingStatus.lastResult.analytics
    } : null
  });
});

async function startServer() {
  console.log('🚀 Starting Planogram Backend v2.0 (Sales Tracking)...');
  await initDatabase();
  app.listen(PORT, () => {
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`📊 Sales tracking: ENABLED`);
    console.log(`🔗 Test at: http://localhost:${PORT}`);
  });
}

// ========================================
// 💰 DISCOUNT CODE CREATION ENDPOINT
// ========================================
app.post('/api/shopify/discount', async (req, res) => {
  const { 
    storeName, 
    accessToken, 
    discountCode, 
    discountType, 
    discountValue,
    startDate,
    endDate,
    variantIds,
    automatic = false
  } = req.body;

  if (!storeName || !accessToken || !discountCode || !discountType || !discountValue) {
    return res.status(400).json({ 
      error: 'Missing required fields',
      required: ['storeName', 'accessToken', 'discountCode', 'discountType', 'discountValue']
    });
  }

  console.log(`🎫 Creating ${automatic ? 'automatic' : 'manual'} discount: ${discountCode}`);

  try {
    const storeNameClean = storeName.replace('.myshopify.com', '');
    
    // Calculate discount value for Shopify API
    let valueType;
    let value;

    if (discountType === 'percentage') {
      valueType = 'percentage';
      value = `-${discountValue}`; // Shopify uses negative values for discounts
    } else {
      valueType = 'fixed_amount';
      value = `-${discountValue}`;
    }

    if (automatic) {
      // Create automatic discount (no code, applies automatically)
      const url = `https://${storeNameClean}.myshopify.com/admin/api/2025-10/price_rules.json`;

      const priceRulePayload = {
        price_rule: {
          title: discountCode,
          target_type: variantIds && variantIds.length > 0 ? 'line_item' : 'line_item',
          target_selection: variantIds && variantIds.length > 0 ? 'entitled' : 'all',
          allocation_method: 'across',
          value_type: valueType,
          value: value,
          customer_selection: 'all',
          starts_at: startDate ? new Date(startDate).toISOString() : new Date().toISOString(),
          ends_at: endDate ? new Date(endDate).toISOString() : null,
          usage_limit: null,
          prerequisite_subtotal_range: null
        }
      };

      // Add variant IDs if targeting specific products
      if (variantIds && variantIds.length > 0) {
        priceRulePayload.price_rule.entitled_variant_ids = variantIds;
      }

      const priceRuleResponse = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Shopify-Access-Token': accessToken,
        },
        body: JSON.stringify(priceRulePayload)
      });

      if (!priceRuleResponse.ok) {
        const errorText = await priceRuleResponse.text();
        console.error('Shopify automatic discount error:', errorText);
        return res.status(priceRuleResponse.status).json({ 
          error: 'Failed to create automatic discount in Shopify',
          details: errorText 
        });
      }

      const priceRuleData = await priceRuleResponse.json();
      const priceRuleId = priceRuleData.price_rule.id;

      console.log(`✅ Automatic discount created: ${priceRuleId}`);

      res.json({
        success: true,
        message: `Automatic discount "${discountCode}" created successfully`,
        priceRuleId: priceRuleId,
        automatic: true
      });

    } else {
      // Create manual discount code (requires code entry)
      const url = `https://${storeNameClean}.myshopify.com/admin/api/2025-10/price_rules.json`;

      const priceRulePayload = {
        price_rule: {
          title: discountCode,
          target_type: variantIds && variantIds.length > 0 ? 'line_item' : 'line_item',
          target_selection: variantIds && variantIds.length > 0 ? 'entitled' : 'all',
          allocation_method: 'across',
          value_type: valueType,
          value: value,
          customer_selection: 'all',
          starts_at: startDate ? new Date(startDate).toISOString() : new Date().toISOString(),
          ends_at: endDate ? new Date(endDate).toISOString() : null,
          usage_limit: null
        }
      };

      // Add variant IDs if targeting specific products
      if (variantIds && variantIds.length > 0) {
        priceRulePayload.price_rule.entitled_variant_ids = variantIds;
      }

      // Create price rule
      const priceRuleResponse = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Shopify-Access-Token': accessToken,
        },
        body: JSON.stringify(priceRulePayload)
      });

      if (!priceRuleResponse.ok) {
        const errorText = await priceRuleResponse.text();
        console.error('Shopify price rule error:', errorText);
        return res.status(priceRuleResponse.status).json({ 
          error: 'Failed to create price rule in Shopify',
          details: errorText 
        });
      }

      const priceRuleData = await priceRuleResponse.json();
      const priceRuleId = priceRuleData.price_rule.id;

      console.log(`✅ Price rule created: ${priceRuleId}`);

      // Create discount code
      const discountCodeUrl = `https://${storeNameClean}.myshopify.com/admin/api/2025-10/price_rules/${priceRuleId}/discount_codes.json`;
      
      const discountCodePayload = {
        discount_code: {
          code: discountCode
        }
      };

      const discountCodeResponse = await fetch(discountCodeUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Shopify-Access-Token': accessToken,
        },
        body: JSON.stringify(discountCodePayload)
      });

      if (!discountCodeResponse.ok) {
        const errorText = await discountCodeResponse.text();
        console.error('Shopify discount code error:', errorText);
        return res.status(discountCodeResponse.status).json({ 
          error: 'Failed to create discount code in Shopify',
          details: errorText 
        });
      }

      const discountCodeData = await discountCodeResponse.json();

      console.log(`✅ Discount code created: ${discountCode}`);

      res.json({
        success: true,
        message: `Discount code "${discountCode}" created successfully`,
        priceRuleId: priceRuleId,
        discountCodeId: discountCodeData.discount_code.id,
        code: discountCodeData.discount_code.code,
        automatic: false
      });
    }

  } catch (error) {
    console.error('❌ Error creating discount:', error);
    res.status(500).json({ 
      error: 'Failed to create discount',
      details: error.message 
    });
  }
});

// ========================================
// 💰 UPDATE PRODUCT PRICES (COMPARE AT PRICE)
// ========================================
app.post('/api/shopify/update-prices', async (req, res) => {
  const { storeName, accessToken, products } = req.body;

  if (!storeName || !accessToken || !products || !Array.isArray(products)) {
    return res.status(400).json({ 
      error: 'Missing required fields',
      required: ['storeName', 'accessToken', 'products (array)']
    });
  }

  console.log(`💰 Updating ${products.length} product prices with compare at price`);

  try {
    const storeNameClean = storeName.replace('.myshopify.com', '');
    let successCount = 0;
    let errorCount = 0;
    const errors = [];

    // Update each product variant
    for (const product of products) {
      if (!product.variantId) {
        errorCount++;
        errors.push(`Missing variant ID for product`);
        continue;
      }

      try {
        const url = `https://${storeNameClean}.myshopify.com/admin/api/2025-10/variants/${product.variantId}.json`;

        const payload = {
          variant: {
            id: product.variantId,
            price: product.newPrice.toFixed(2),
            compare_at_price: product.compareAtPrice.toFixed(2)
          }
        };

        const response = await fetch(url, {
          method: 'PUT',
          headers: {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': accessToken,
          },
          body: JSON.stringify(payload)
        });

        if (response.ok) {
          successCount++;
          console.log(`✅ Updated variant ${product.variantId}: $${product.newPrice} (was $${product.compareAtPrice})`);
        } else {
          const errorText = await response.text();
          errorCount++;
          errors.push(`Variant ${product.variantId}: ${errorText}`);
          console.error(`❌ Failed to update variant ${product.variantId}:`, errorText);
        }

        // Rate limiting: small delay between requests
        await new Promise(resolve => setTimeout(resolve, 200));

      } catch (error) {
        errorCount++;
        errors.push(`Variant ${product.variantId}: ${error.message}`);
        console.error(`❌ Error updating variant ${product.variantId}:`, error);
      }
    }

    console.log(`✅ Price update complete: ${successCount} succeeded, ${errorCount} failed`);

    res.json({
      success: true,
      message: `Updated ${successCount} of ${products.length} products`,
      updated: successCount,
      failed: errorCount,
      errors: errors.length > 0 ? errors : undefined
    });

  } catch (error) {
    console.error('❌ Error updating prices:', error);
    res.status(500).json({ 
      error: 'Failed to update prices',
      details: error.message 
    });
  }
});

// ============================================
// INVENTORY SYNC ENDPOINTS
// ============================================

// Manual inventory sync endpoint with auth
app.post('/api/sync-inventory', async (req, res) => {
  try {
    const { storeName, accessToken } = req.body;
    
    if (!storeName || !accessToken) {
      return res.status(400).json({ 
        success: false, 
        error: 'Missing storeName or accessToken' 
      });
    }
    
    console.log('🔄 Manual inventory sync triggered');
    const result = await updateInventoryLevels(storeName, accessToken);
    
    res.json({
      success: true,
      message: 'Inventory sync completed',
      ...result
    });
  } catch (error) {
    console.error('❌ Manual sync failed:', error);
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// Quick sync endpoint (uses env vars for convenience)
app.post('/api/sync-inventory-quick', async (req, res) => {
  try {
    const storeName = process.env.SHOPIFY_STORE_NAME || 'a1beautysupply.myshopify.com';
    const accessToken = process.env.SHOPIFY_ACCESS_TOKEN;
    
    if (!accessToken) {
      return res.status(400).json({ 
        success: false, 
        error: 'No access token configured in environment variables' 
      });
    }
    
    console.log('🔄 Quick inventory sync triggered');
    const result = await updateInventoryLevels(storeName, accessToken);
    
    res.json({
      success: true,
      message: 'Inventory sync completed',
      ...result
    });
  } catch (error) {
    console.error('❌ Quick sync failed:', error);
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// Get last sync status
app.get('/api/sync-status', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total_products,
        MAX(updated_at) as last_inventory_update,
        COUNT(CASE WHEN inventory_quantity > 0 THEN 1 END) as in_stock,
        COUNT(CASE WHEN inventory_quantity = 0 THEN 1 END) as out_of_stock
      FROM products
    `);
    
    res.json({
      success: true,
      ...result.rows[0]
    });
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// ============================================
// AUTO-SYNC INVENTORY
// ============================================

const AUTO_SYNC_INTERVAL = 60 * 60 * 1000; // 1 hour

async function autoSyncInventory() {
  const storeName = process.env.SHOPIFY_STORE_NAME || 'a1beautysupply.myshopify.com';
  const accessToken = process.env.SHOPIFY_ACCESS_TOKEN;
  
  if (!accessToken) {
    console.log('⚠️ No access token configured for auto-sync');
    return;
  }
  
  try {
    console.log('⏰ Running scheduled inventory sync...');
    const result = await updateInventoryLevels(storeName, accessToken);
    console.log(`✅ Scheduled sync complete: ${result.updated}/${result.total} updated`);
  } catch (error) {
    console.error('❌ Scheduled sync failed:', error.message);
  }
}

// Run initial sync 30 seconds after server starts
setTimeout(() => {
  console.log('🚀 Running initial inventory sync...');
  autoSyncInventory();
}, 30000);

// Then run every hour
setInterval(autoSyncInventory, AUTO_SYNC_INTERVAL);
console.log(`⏰ Auto-sync enabled: will run every ${AUTO_SYNC_INTERVAL / 1000 / 60} minutes`);

pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('❌ Database connection failed:', err);
  } else {
    console.log('✅ Database connected successfully');
    console.log(`📅 Server time: ${res.rows[0].now}`);
  }
});

// Auto-sync inventory every 5 minutes
const FIVE_MINUTES = 5 * 60 * 1000;

setInterval(async () => {
  const storeName = process.env.SHOPIFY_STORE_NAME || '1m3tfh-xt.myshopify.com';
  const accessToken = process.env.SHOPIFY_ACCESS_TOKEN;
  
  if (accessToken) {
    console.log('⏰ Running automatic 5-minute inventory sync...');
    try {
      await updateInventoryLevels(storeName, accessToken);
      console.log('✅ Auto-sync complete');
    } catch (error) {
      console.error('❌ Auto-sync failed:', error.message);
    }
  }
}, FIVE_MINUTES);

console.log('⏰ Automatic inventory sync enabled: every 5 minutes');

startServer();
