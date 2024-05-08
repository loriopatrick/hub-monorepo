use super::{HubError, RootPrefix, UserPostfix, TS_HASH_LENGTH};

#[derive(Clone)]
#[repr(C, packed)]
pub struct UserKeyHeader {
    _root: RootPrefix,
    fid_be: u32,
    msg_type: UserPostfix,
}

impl UserKeyHeader {
    pub fn new(fid: u32, msg_type: UserPostfix) -> Self {
        UserKeyHeader {
            _root: RootPrefix::User,
            fid_be: fid.to_be(),
            msg_type,
        }
    }

    pub fn from_slice<'a>(slice: &'a [u8]) -> Option<&'a Self> {
        if slice.len() < std::mem::size_of::<Self>() {
            return None;
        }
        Some(unsafe { &*(slice.as_ptr() as *const u8 as *const Self) })
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            let ptr = self as *const Self as *const u8;
            std::slice::from_raw_parts(ptr, std::mem::size_of::<UserKeyHeader>())
        }
    }
}

#[repr(C, packed)]
pub struct MessagePrimaryKey {
    user_header: UserKeyHeader,
    timestamp_be: u32,
    content_hash: [u8; 20],
}

impl MessagePrimaryKey {
    pub fn new(fid: u32, msg_type: UserPostfix, hash: &[u8]) -> Result<Self, HubError> {
        if hash.len() < 24 {
            return Err(HubError {
                code: "bad_request.internal_error".to_string(),
                message: format!("message_ts_hash is not 24 bytes: {:x?}", vec),
            });
        }

        Ok(MessagePrimaryKey {
            user_header: UserKeyHeader::new(fid, msg_type),
            timestamp_be: u32::from_be_bytes(hash[..4].try_into().unwrap()).to_be(),
            content_hash: hash[4..].try_into().unwrap(),
        })
    }

    pub fn from_slice<'a>(slice: &'a [u8]) -> Option<&'a Self> {
        if slice.len() < std::mem::size_of::<Self>() {
            return None;
        }
        Some(unsafe { &*(slice.as_ptr() as *const u8 as *const Self) })
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            let ptr = self as *const Self as *const u8;
            std::slice::from_raw_parts(ptr, std::mem::size_of::<UserKeyHeader>())
        }
    }

    pub fn user_header(&self) -> &UserKeyHeader {
        &self.user_header
    }

    pub fn timestamp_hash(&self) -> &[u8; TS_HASH_LENGTH] {
        let ptr = self as *const Self as *const u8;
        unsafe { &*(ptr.add(std::mem::size_of::<UserKeyHeader>()) as *const [u8; TS_HASH_LENGTH]) }
    }
}

#[repr(C, packed)]
pub struct MessageActionKey {
    user_header: UserKeyHeader,
    content_hash: [u8; 20],
}

impl MessageActionKey {
    pub fn new(fid: u32, msg_type: UserPostfix, hash: &[u8]) -> Result<Self, HubError> {
        if hash.len() != 20 {
            return Err(HubError {
                code: "bad_request.internal_error".to_string(),
                message: format!("message_ts_hash is not 20 bytes: {:x?}", vec),
            });
        }

        Ok(MessageActionKey {
            user_header: UserKeyHeader::new(fid, msg_type),
            content_hash: hash.try_into().unwrap(),
        })
    }

    pub fn from_slice<'a>(slice: &'a [u8]) -> Option<&'a Self> {
        if slice.len() < std::mem::size_of::<Self>() {
            return None;
        }
        Some(unsafe { &*(slice.as_ptr() as *const u8 as *const Self) })
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            let ptr = self as *const Self as *const u8;
            std::slice::from_raw_parts(ptr, std::mem::size_of::<UserKeyHeader>())
        }
    }

    pub fn user_header(&self) -> &UserKeyHeader {
        &self.user_header
    }
}

impl AsRef<[u8]> for MessageActionKey {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
