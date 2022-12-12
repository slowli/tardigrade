//! Helper macros.

macro_rules! delegate_read_traits {
    ($name:ty { $field:ident $(: $field_ty:ident)? }) => {
        #[async_trait::async_trait]
        impl$(<$field_ty>)? $crate::storage::ReadModules for $name
        $(where
            $field_ty: $crate::storage::ReadonlyStorageTransaction,)?
        {
            async fn module(&self, id: &str) -> Option<$crate::storage::ModuleRecord> {
                self.$field.module(id).await
            }

            fn modules(&self) -> futures::stream::BoxStream<'_, $crate::storage::ModuleRecord> {
                self.$field.modules()
            }
        }

        #[async_trait::async_trait]
        impl$(<$field_ty>)? $crate::storage::ReadChannels for $name
        $(where
            $field_ty: $crate::storage::ReadonlyStorageTransaction,)?
        {
            async fn channel(
                &self,
                id: tardigrade::ChannelId,
            ) -> Option<$crate::storage::ChannelRecord> {
                self.$field.channel(id).await
            }

            async fn channel_message(
                &self,
                id: tardigrade::ChannelId,
                index: usize,
            ) -> Result<Vec<u8>, $crate::storage::MessageError> {
                self.$field.channel_message(id, index).await
            }

            fn channel_messages(
                &self,
                id: tardigrade::ChannelId,
                indices: std::ops::RangeInclusive<usize>,
            ) -> futures::stream::BoxStream<'_, (usize, Vec<u8>)> {
                self.$field.channel_messages(id, indices)
            }
        }

        #[async_trait::async_trait]
        impl$(<$field_ty>)? $crate::storage::ReadWorkflows for $name
        $(where
            $field_ty: $crate::storage::ReadonlyStorageTransaction,)?
        {
            async fn count_active_workflows(&self) -> usize {
                self.$field.count_active_workflows().await
            }

            async fn workflow(
                &self,
                id: tardigrade::WorkflowId,
            ) -> Option<$crate::storage::WorkflowRecord> {
                self.$field.workflow(id).await
            }

            async fn nearest_timer_expiration(&self) -> Option<chrono::DateTime<chrono::Utc>> {
                self.$field.nearest_timer_expiration().await
            }
        }
    };
}
