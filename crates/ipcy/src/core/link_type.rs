//! Port of `ipcz/link_type.{h,cc}`.

use std::fmt;

/// Indicates what role a specific RouterLink plays along its route. Every
/// end-to-end route has exactly one "central" link -- the link that connects
/// one side of the route to the other -- along with any number of "peripheral"
/// links extending the route away from the central link on one side or the
/// other.
///
/// If a peripheral link connects a router R1 to another router R2 which is
/// closer to the central link, the link is considered to be "outward" from R1
/// and "inward" from R2.
///
/// When two routes are joined via the MergePortals API, the merged portals'
/// routers are linked together via a bridge link.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum LinkType {
    /// The link along a route which connects one side of the route to the
    /// other. This is the only link which is treated by both sides as an
    /// outward link, and it's the only link along a route at which decay can be
    /// initiated by a router.
    #[default]
    Central,

    /// Peripheral links extend the route on one side. They forward parcels and
    /// other messages along the same direction in which they were received, and
    /// can only decay as part of a decaying process initiated on a central link
    /// by a mutually adjacent router. An inward peripheral link goes further
    /// toward the terminal endpoint of the router's own side of the route, while
    /// an outward peripheral link goes toward the terminal endpoint of the side
    /// opposite the router.
    PeripheralInward,
    PeripheralOutward,

    /// Bridge links are formed only when merging two routes together. A bridge
    /// link links two terminal routers from two different routes, and it can
    /// only decay once both routers are adjacent to decayable central links
    /// along their own respective routes.
    Bridge,
}

impl LinkType {
    pub fn is_outward(self) -> bool {
        self.is_central() || self.is_peripheral_outward()
    }

    pub fn is_central(self) -> bool {
        self == LinkType::Central
    }

    pub fn is_peripheral_inward(self) -> bool {
        self == LinkType::PeripheralInward
    }

    pub fn is_peripheral_outward(self) -> bool {
        self == LinkType::PeripheralOutward
    }

    pub fn is_bridge(self) -> bool {
        self == LinkType::Bridge
    }
}

impl fmt::Display for LinkType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LinkType::Central => "central",
            LinkType::PeripheralInward => "peripheral-inward",
            LinkType::PeripheralOutward => "peripheral-outward",
            LinkType::Bridge => "bridge",
        })
    }
}
