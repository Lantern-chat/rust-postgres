use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::types;
use std::error::Error;

use crate::{FromSql, IsNull, Kind, ToSql, Type};

use thin_vec_02::ThinVec;

impl<'a, T> FromSql<'a> for ThinVec<T>
where
    T: FromSql<'a>,
{
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        let array = types::array_from_sql(raw)?;
        if array.dimensions().count()? > 1 {
            return Err("array contains too many dimensions".into());
        }

        // FallibleIterator is not implemented for `ThinVec`, so do it manually

        let mut values = array.values();
        let mut v = ThinVec::with_capacity(values.size_hint().0);

        while let Some(value) = values.next()? {
            v.push(T::from_sql_nullable(member_type, value)?);
        }

        Ok(v)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Array(ref inner) => T::accepts(inner),
            _ => false,
        }
    }
}

impl<T: ToSql> ToSql for ThinVec<T> {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[T] as ToSql>::to_sql(&&**self, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&[T] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}
