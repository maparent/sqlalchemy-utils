from collections import defaultdict
from collections.abc import Iterable

import six
import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import attributes, class_mapper, ColumnProperty, sync, util as mapperutil
from sqlalchemy.orm.interfaces import MapperProperty, PropComparator, MANYTOONE
from sqlalchemy.orm.session import _state_session
from sqlalchemy.orm.dependency import ManyToOneDP, DependencyProcessor
from sqlalchemy.util import set_creation_order, warn

from .exceptions import ImproperlyConfigured
from .functions import identity


class TypeMapper(object):

    def class_to_value(self, cls):
        return six.text_type(cls.__name__)

    def column_is_type(self, column, other_type):
        mapper = sa.inspect(other_type)
        # Iterate through the weak sequence in order to get the actual
        # mappers
        class_names = [self.class_to_value(other_type)]
        class_names.extend([
            self.class_to_value(submapper.class_)
            for submapper in mapper._inheriting_mappers
        ])
        return column.in_(class_names)

    def value_to_class(self, value, base_class):
        return base_class._decl_class_registry.get(value)


class GenericAttributeImpl(attributes.ScalarObjectAttributeImpl):
    def get(self, state, dict_, passive=attributes.PASSIVE_OFF):
        if self.key in dict_:
            return dict_[self.key]

        # Retrieve the session bound to the state in order to perform
        # a lazy query for the attribute.
        session = _state_session(state)
        if session is None:
            # State is not bound to a session; we cannot proceed.
            return None

        # Find class for discriminator.
        # TODO: Perhaps optimize with some sort of lookup?
        discriminator = self.get_state_discriminator(state)
        target_class = self.parent_token.type_mapper.value_to_class(discriminator, state.class_)

        if target_class is None:
            # Unknown discriminator; return nothing.
            return None

        id = self.get_state_id(state)

        target = session.query(target_class).get(id)

        # Return found (or not found) target.
        return target

    def get_state_discriminator(self, state):
        discriminator = self.parent_token.discriminator
        if isinstance(discriminator, hybrid_property):
            return getattr(state.obj(), discriminator.__name__)
        else:
            return state.attrs[discriminator.key].value

    def get_state_id(self, state):
        # Lookup row with the discriminator and id.
        return tuple(state.attrs[id.key].value for id in self.parent_token.id)


    def set(self, state, dict_, initiator,
            passive=attributes.PASSIVE_OFF,
            check_old=None,
            pop=False):

        # Set us on the state.
        dict_[self.key] = initiator

        if initiator is None:
            # Nullify relationship args
            for id in self.parent_token.id:
                dict_[id.key] = None
            dict_[self.parent_token.discriminator.key] = None
        else:
            # Get the primary key of the initiator and ensure we
            # can support this assignment.
            class_ = type(initiator)
            mapper = class_mapper(class_)

            pk = mapper.identity_key_from_instance(initiator)[1]

            # Set the identifier and the discriminator.
            discriminator = self.parent_token.type_mapper.class_to_value(class_)

            if all(pk):
                # delay
                for index, id in enumerate(self.parent_token.id):
                    dict_[id.key] = pk[index]
            dict_[self.parent_token.discriminator.key] = discriminator


class GenericManyToOneDP(ManyToOneDP):

    def __init__(self, prop):
        super(GenericManyToOneDP, self).__init__(prop)
        self.mapper._dependency_processors.append(self)
        self.mapper = self.prop.mapper   # temporary
        # NOTE should I make it into a threadlocal property? or lock?

    def mapper_for_state(self, state):
        # TODO: using the object itself?
        val = getattr(state._strong_obj, self.prop.discriminator.key)
        if val:
            cls = self.prop.type_mapper.value_to_class(val, self.parent.class_)
            if cls:
                return cls.__mapper__

    def group_states(self, states):
        states_d = defaultdict(list)
        for state in states:
            mapper = self.mapper_for_state(state)
            states_d[mapper].append(state)
        return states_d.items()

    def process_saves(self, uowcommit, states):
        for mapper, states in self.group_states(states):
            self.mapper = mapper
            for state in states:
                obj = state._strong_obj
                keys = [c.key for c in self.prop.id]
                if not all([getattr(obj, key) for key in keys]):
                    target = getattr(obj, self.prop.key)
                    if target:
                        vals = mapper.identity_key_from_instance(target)[1]
                        for key, val in zip(keys, vals):
                            setattr(obj, key, val)
            super(GenericManyToOneDP, self).process_saves(uowcommit, states)

    def process_deletes(self, uowcommit, states):
        for mapper, states in self.group_states(states):
            self.mapper = mapper
            super(GenericManyToOneDP, self).process_saves(uowcommit, states)

    def prop_has_changes(self, uowcommit, states, isdelete):
        for mapper, states in self.group_states(states):
            self.mapper = mapper
            if super(GenericManyToOneDP, self).prop_has_changes(
                    uowcommit, states, isdelete):
                return True

    def presort_deletes(self, uowcommit, states):
        for mapper, states in self.group_states(states):
            self.mapper = mapper
            super(GenericManyToOneDP, self).presort_deletes(uowcommit, states)

    def presort_saves(self, uowcommit, states):
        for mapper, states in self.group_states(states):
            self.mapper = mapper
            super(GenericManyToOneDP, self).presort_saves(uowcommit, states)

    def per_property_flush_actions(self, uow):
        my_states = [state for state in uow.states if state.mapper == self.parent]
        for mapper, states in self.group_states(my_states):
            self.mapper = mapper
            super(GenericManyToOneDP, self).per_property_flush_actions(uow)

    def per_state_flush_actions(self, uow, states, isdelete):
        for mapper, states in self.group_states(states):
            self.mapper = mapper
            super(GenericManyToOneDP, self).per_state_flush_actions(uow, states, isdelete)

    def _verify_canload(self, state):
        self.mapper = self.mapper_for_state(state)
        return super(GenericManyToOneDP, self)._verify_canload(state)

    def _synchronize(self, state, child, associationrow,
                     clearkeys, uowcommit, operation=None):
        if state is None or \
                (not self.post_update and uowcommit.is_deleted(state)):
            return

        if operation is not None and \
                child is not None and \
                not uowcommit.session._contains_state(child):
            warn(
                "Object of type %s not in session, %s "
                "operation along '%s' won't proceed" %
                (mapperutil.state_class_str(child), operation, self.prop))
            return

        synchronize_pairs = (
            child.__class__.__mapper__.primary_key[0],
            self.id)
        if clearkeys or child is None:
            sync.clear(state, self.parent, synchronize_pairs)
        else:
            self._verify_canload(child)
            mapper = self.mapper_for_state(state)
            sync.populate(child, mapper, state,
                          self.parent,
                          synchronize_pairs,
                          uowcommit,
                          False)


class GenericRelationshipProperty(MapperProperty):
    """A generic form of the relationship property.

    Creates a 1 to many relationship between the parent model
    and any other models using a descriminator (the table name).

    :param discriminator
        Field to discriminate which model we are referring to.
    :param id:
        Field to point to the model we are referring to.
    """

    def __init__(self, discriminator, id, type_mapper=None, doc=None):
        super(GenericRelationshipProperty, self).__init__()
        self._discriminator_col = discriminator
        self.type_mapper = type_mapper or TypeMapper()
        self._id_cols = id
        self._id = None
        self._discriminator = None
        self.doc = doc
        self.direction = MANYTOONE
        self.uselist = False
        self.cascade = mapperutil.CascadeOptions("save-update, merge")
        self.secondary = None
        self.post_update = False
        self.passive_deletes = False
        self.passive_updates = False
        self.enable_typechecks = False
        self.synchronize_pairs = ((None, id),)
        self._reverse_property = None
        set_creation_order(self)

    def _column_to_property(self, column):
        if isinstance(column, hybrid_property):
            attr_key = column.__name__
            for key, attr in self.parent.all_orm_descriptors.items():
                if key == attr_key:
                    return attr
        else:
            for key, attr in self.parent.attrs.items():
                if isinstance(attr, ColumnProperty):
                    if attr.columns[0].name == column.name:
                        return attr

    def do_init(self):
        super(GenericRelationshipProperty, self).do_init()

        def convert_strings(column):
            if isinstance(column, six.string_types):
                return self.parent.columns[column]
            return column

        self._discriminator_col = convert_strings(self._discriminator_col)
        self._id_cols = convert_strings(self._id_cols)

        if isinstance(self._id_cols, Iterable):
            self._id_cols = list(map(convert_strings, self._id_cols))
        else:
            self._id_cols = [self._id_cols]

        self.discriminator = self._column_to_property(self._discriminator_col)

        if self.discriminator is None:
            raise ImproperlyConfigured(
                'Could not find discriminator descriptor.'
            )

        self.id = list(map(self._column_to_property, self._id_cols))
        self.mapper = self.discriminator.parent  # temporary
        self._dependency_processor = GenericManyToOneDP(self)
        self._dependency_processor.cascade = self.cascade

    def cascade_iterator(self, type_, state, dict_,
                         visited_states, halt_on=None):
        passive = attributes.PASSIVE_NO_INITIALIZE

        if type_ == 'save-update':
            tuples = state.manager[self.key].impl.\
                get_all_pending(state, dict_)
        else:
            tuples = self._value_as_iterable(state, dict_, self.key,
                                             passive=passive)

        for instance_state, c in tuples:
            if instance_state in visited_states:
                continue

            if c is None:
                # would like to emit a warning here, but
                # would not be consistent with collection.append(None)
                # current behavior of silently skipping.
                # see [ticket:2229]
                continue

            instance_dict = attributes.instance_dict(c)

            if halt_on and halt_on(instance_state):
                continue

            instance_mapper = instance_state.manager.mapper

            visited_states.add(instance_state)

            yield c, instance_mapper, instance_state, instance_dict

    class Comparator(PropComparator):
        def __init__(self, prop, parentmapper):
            self.property = prop
            self._parententity = parentmapper

        def __eq__(self, other):
            discriminator = self.property.type_mapper.class_to_value(type(other))
            q = self.property._discriminator_col == discriminator
            other_id = identity(other)
            for index, id in enumerate(self.property._id_cols):
                q &= id == other_id[index]
            return q

        def __ne__(self, other):
            return ~(self == other)

        def is_type(self, other):
            return self.property.type_mapper.column_is_type(
                self.property._discriminator_col, other)

    def instrument_class(self, mapper):
        attributes.register_attribute(
            mapper.class_,
            self.key,
            comparator=self.Comparator(self, mapper),
            parententity=mapper,
            doc=self.doc,
            impl_class=GenericAttributeImpl,
            parent_token=self
        )


def generic_relationship(*args, **kwargs):
    return GenericRelationshipProperty(*args, **kwargs)
