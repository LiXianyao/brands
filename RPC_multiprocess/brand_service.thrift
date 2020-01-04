/**
 * Thrift files can namespace, package, or prefix their output in various
 * target languages.
 */
namespace cpp brand_service
namespace d brand_service
namespace dart brand_service
namespace java brand_service
namespace php brand_service
namespace perl brand_service
namespace haxe brand_service
namespace netcore brand_service
struct tuple{
  1: string name,
  2: optional string probability,
}

struct groupRes {
  1: i16 classNo,
  2: list<string> similarNameList,
  3: list<tuple> similarNameProb,
  4: list<tuple> itemSucessRate,
}


/**
 * Structs can also be exceptions, if they are nasty.
 */
exception TException {
  1: i32 whatOp,
  2: string why
}

/**
 * Ahh, now onto the cool part, defining a service. Services just need a name
 * and can optionally inherit from another service using the extends keyword.
 */
service BrandSearch{

   oneway void stop(),

   map<i16, groupRes> queryBrand(1:string inputJson) throws (1:TException ouch),

    void reload()

}