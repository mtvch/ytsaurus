#include "raw_transform.h"

#include "../type_tag.h"

namespace NRoren::NPrivate {

template <typename T>
T* CheckedCast(IRawTransform* t)
{
    auto result = dynamic_cast<T*>(t);
    Y_ABORT_UNLESS(result);
    return result;
}

template <typename T>
const T* CheckedCast(const IRawTransform* t)
{
    auto result = dynamic_cast<const T*>(t);
    Y_ABORT_UNLESS(result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

IRawReadPtr IRawTransform::AsRawRead()
{
    return CheckedCast<IRawRead>(this);
}

IRawWritePtr IRawTransform::AsRawWrite()
{
    return CheckedCast<IRawWrite>(this);
}

IRawParDoPtr IRawTransform::AsRawParDo()
{
    return CheckedCast<IRawParDo>(this);
}

IRawStatefulParDoPtr IRawTransform::AsRawStatefulParDo()
{
    return CheckedCast<IRawStatefulParDo>(this);
}

IRawStatefulTimerParDoPtr IRawTransform::AsRawStatefulTimerParDo()
{
    return CheckedCast<IRawStatefulTimerParDo>(this);
}

IRawGroupByKeyPtr IRawTransform::AsRawGroupByKey()
{
    return CheckedCast<IRawGroupByKey>(this);
}

IRawCombinePtr IRawTransform::AsRawCombine()
{
    return CheckedCast<IRawCombine>(this);
}

IRawCoGroupByKeyPtr IRawTransform::AsRawCoGroupByKey()
{
    return CheckedCast<IRawCoGroupByKey>(this);
}

IRawFlattenPtr IRawTransform::AsRawFlatten()
{
    return CheckedCast<IRawFlatten>(this);
}

const IRawRead& IRawTransform::AsRawReadRef() const
{
    return *CheckedCast<IRawRead>(this);
}

const IRawWrite& IRawTransform::AsRawWriteRef() const
{
    return *CheckedCast<IRawWrite>(this);
}

const IRawParDo& IRawTransform::AsRawParDoRef() const
{
    return *CheckedCast<IRawParDo>(this);
}

const IRawStatefulParDo& IRawTransform::AsRawStatefulParDoRef() const
{
    return *CheckedCast<IRawStatefulParDo>(this);
}

const IRawStatefulTimerParDo& IRawTransform::AsRawStatefulTimerParDoRef() const
{
    return *CheckedCast<IRawStatefulTimerParDo>(this);
}

const IRawGroupByKey& IRawTransform::AsRawGroupByKeyRef() const
{
    return *CheckedCast<IRawGroupByKey>(this);
}

const IRawCombine& IRawTransform::AsRawCombineRef() const
{
    return *CheckedCast<IRawCombine>(this);
}

const IRawCoGroupByKey& IRawTransform::AsRawCoGroupByKeyRef() const
{
    return *CheckedCast<IRawCoGroupByKey>(this);
}

const IRawFlatten& IRawTransform::AsRawFlattenRef() const {
    return *CheckedCast<IRawFlatten>(this);
}

////////////////////////////////////////////////////////////////////////////////

TRawBaseTransform::TRawBaseTransform(const ERawTransformType type, std::vector<TDynamicTypeTag> inputs, std::vector<TDynamicTypeTag> outputs)
    : Type_(type)
    , Inputs_(std::move(inputs))
    , Outputs_(std::move(outputs))
{
}

TRawBaseTransform::TRawBaseTransform(const ERawTransformType type, const std::vector<TRowVtable>& inputs, const std::vector<TRowVtable>& outputs)
    : Type_(type)
{
    Inputs_.reserve(inputs.size());
    for (ssize_t i = 0; i < std::ssize(inputs); ++i) {
        Inputs_.push_back(NRoren::TDynamicTypeTag(NYT::Format("intput-%v", i), inputs[i]));
    }
    Outputs_.reserve(outputs.size());
    for (ssize_t i = 0; i < std::ssize(outputs); ++i) {
        Outputs_.push_back(NRoren::TDynamicTypeTag(NYT::Format("output-%v", i), outputs[i]));
    }
}

ERawTransformType TRawBaseTransform::GetType() const
{
    return Type_;
}

ISerializable<TRawBaseTransform>::TDefaultFactoryFunc TRawBaseTransform::GetDefaultFactory() const
{
    return []() { return NYT::New<TRawBaseTransform>(ERawTransformType::Invalid); };
}

std::vector<TDynamicTypeTag> TRawBaseTransform::GetInputTags() const
{
    return Inputs_;
}

std::vector<TDynamicTypeTag> TRawBaseTransform::GetOutputTags() const
{
    return Outputs_;
}

////////////////////////////////////////////////////////////////////////////////
TRawMetaTransform::TRawMetaTransform(const ERawTransformType type, std::vector<TDynamicTypeTag> inputs, std::vector<TDynamicTypeTag> outputs)
    : TRawBaseTransform(type, std::move(inputs), std::move(outputs))
{ }

TRawMetaTransform::TRawMetaTransform(const ERawTransformType type, const std::vector<TRowVtable>& inputs, const std::vector<TRowVtable>& outputs)
    : TRawBaseTransform(type, std::move(inputs), std::move(outputs))
{ }

////////////////////////////////////////////////////////////////////////////////

TRawDummyRead::TRawDummyRead(TRowVtable vtable)
    : Vtable_(std::move(vtable))
{ }

const void* TRawDummyRead::NextRaw()
{
    Y_ABORT("unimplemented method of TRawDummyRead");
}

ISerializable<IRawRead>::TDefaultFactoryFunc TRawDummyRead::GetDefaultFactory() const
{
    return []() -> NPrivate::IRawReadPtr {
        return NYT::New<TRawDummyRead>();
    };
}

void TRawDummyRead::Save(IOutputStream* out) const
{
    ::Save(out, Vtable_);
}

void TRawDummyRead::Load(IInputStream* in)
{
    ::Load(in, Vtable_);
}

std::vector<TDynamicTypeTag> TRawDummyRead::GetInputTags() const
{
    return {};
}

std::vector<TDynamicTypeTag> TRawDummyRead::GetOutputTags() const
{
    return std::vector<TDynamicTypeTag>{TDynamicTypeTag("dummy-read-output-0", Vtable_)};
}

////////////////////////////////////////////////////////////////////////////////

TRawDummyWriter::TRawDummyWriter(TRowVtable vtable)
    : Vtable_(std::move(vtable))
{ }

void TRawDummyWriter::AddRaw(const void*, ssize_t)
{
    Y_ABORT("not implemented");
}

void TRawDummyWriter::Close()
{
    Y_ABORT("not implemented");
}

ISerializable<IRawWrite>::TDefaultFactoryFunc TRawDummyWriter::GetDefaultFactory() const
{
    return []() -> IRawWritePtr {
        auto result = NYT::New<TRawDummyWriter>();
        return result.Get();
    };
}

void TRawDummyWriter::Save(IOutputStream* out) const
{
    ::Save(out, Vtable_);
}

void TRawDummyWriter::Load(IInputStream* in)
{
    ::Load(in, Vtable_);
}

std::vector<TDynamicTypeTag> TRawDummyWriter::GetInputTags() const
{
    return std::vector<TDynamicTypeTag>{TDynamicTypeTag("dummy-write-input-0", Vtable_)};
}

std::vector<TDynamicTypeTag> TRawDummyWriter::GetOutputTags() const
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
