use crate::{
    schema::{
        column_description::dsl as cd_d, description_binary::dsl as desc_b,
        description_datetime::dsl as desc_d, description_enum::dsl as desc_e,
        description_float::dsl as desc_f, description_int::dsl as desc_i,
        description_ipaddr::dsl as desc_ip, description_text::dsl as desc_t,
        time_series::dsl as ts_d, top_n_binary::dsl as t_b, top_n_datetime::dsl as t_d,
        top_n_enum::dsl as t_e, top_n_float::dsl as t_f, top_n_int::dsl as t_i,
        top_n_ipaddr::dsl as t_ip, top_n_text::dsl as t_t,
    },
    Database,
};
use anyhow::Result;
use chrono::NaiveDateTime;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;

impl Database {
    /// Delete the statistics older than retention time.
    ///
    /// # Errors
    ///
    /// Returns an error if statistics fails to delete
    pub async fn retain_column_statistics(&self, stat_retention: NaiveDateTime) -> Result<()> {
        let mut conn = self.pool.get_diesel_conn().await?;

        // delete time_series older than retention with time
        diesel::delete(ts_d::time_series.filter(ts_d::time.lt(stat_retention)))
            .execute(&mut conn)
            .await?;

        // get the column_description id older than retention with time.
        let query = cd_d::column_description
            .select(cd_d::id)
            .filter(cd_d::batch_ts.lt(stat_retention));
        let cd_ids = query.load::<i32>(&mut conn).await?;

        // delete top_n_binary, description_binary with description_id.
        diesel::delete(t_b::top_n_binary.filter(t_b::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;
        diesel::delete(desc_b::description_binary.filter(desc_b::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;

        // delete top_n_datetime, description_datetime with description_id.
        diesel::delete(t_d::top_n_datetime.filter(t_d::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;
        diesel::delete(desc_d::description_datetime.filter(desc_d::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;

        // delete top_n_enum, description_enum with description_id.
        diesel::delete(t_e::top_n_enum.filter(t_e::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;
        diesel::delete(desc_e::description_enum.filter(desc_e::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;

        // delete top_n_float, description_float with description_id.
        diesel::delete(t_f::top_n_float.filter(t_f::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;
        diesel::delete(desc_f::description_float.filter(desc_f::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;

        // delete top_n_int, description_int with description_id.
        diesel::delete(t_i::top_n_int.filter(t_i::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;
        diesel::delete(desc_i::description_int.filter(desc_i::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;

        // delete top_n_ipaddr, description_ipaddr with description_id.
        diesel::delete(t_ip::top_n_ipaddr.filter(t_ip::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;
        diesel::delete(desc_ip::description_ipaddr.filter(desc_ip::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;

        // delete top_n_text, description_text with description_id.
        diesel::delete(t_t::top_n_text.filter(t_t::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;
        diesel::delete(desc_t::description_text.filter(desc_t::description_id.eq_any(&cd_ids)))
            .execute(&mut conn)
            .await?;

        // delete column_description oplder than retention with batch_ts
        diesel::delete(cd_d::column_description.filter(cd_d::batch_ts.lt(stat_retention)))
            .execute(&mut conn)
            .await?;

        Ok(())
    }
}
