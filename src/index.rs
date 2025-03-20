//! 二级索引
//!

use crate::protos::table_store::{IndexMeta, IndexSyncPhase, IndexType, IndexUpdateMode};

/// Builder for [`IndexMeta`]
#[derive(Debug, Clone, Default)]
pub struct IndexMetaBuilder {
    idx_meta: IndexMeta,
}

impl IndexMetaBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            idx_meta: IndexMeta {
                name: name.to_string(),
                ..Default::default()
            },
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.idx_meta.name = name.into();
        self
    }

    pub fn add_primary_key(mut self, pk_name: &str) -> Self {
        self.idx_meta.primary_key.push(pk_name.into());
        self
    }

    pub fn add_defined_column(mut self, col_name: &str) -> Self {
        self.idx_meta.defined_column.push(col_name.into());
        self
    }

    pub fn index_update_mode(mut self, mode: IndexUpdateMode) -> Self {
        self.idx_meta.index_update_mode = mode as i32;
        self
    }

    pub fn index_type(mut self, idx_type: IndexType) -> Self {
        self.idx_meta.index_type = idx_type as i32;
        self
    }

    pub fn index_sync_phase(mut self, phase: IndexSyncPhase) -> Self {
        self.idx_meta.index_sync_phase = Some(phase as i32);
        self
    }

    pub fn build(self) -> IndexMeta {
        self.idx_meta
    }
}

/// Add `builder` method to [`IndexMeta`]
impl IndexMeta {
    pub fn builder(name: &str) -> IndexMetaBuilder {
        IndexMetaBuilder::new(name)
    }
}
